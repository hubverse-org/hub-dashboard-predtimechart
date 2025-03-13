import datetime
import itertools
import json
from collections import defaultdict
from pathlib import Path
from typing import Optional

import pandas as pd
import polars as pl
import yaml
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from hub_predtimechart.ptc_schema import ptc_config_schema


class HubConfig:
    """
    Provides various visualization-related variables that are parsed from various places in `hub_dir`.
    """


    def __init__(self, hub_dir: Path, ptc_config_file: Path):
        """
        :param hub_dir: Path to a hub's root directory. see: https://hubverse.io/en/latest/user-guide/hub-structure.html
        :param ptc_config_file: (input) a file Path to a `predtimechart-config.yaml` file that specifies how to process
            `hub_dir` to get predtimechart output
        """
        super().__init__()

        # check for hub_dir
        if not hub_dir.exists():
            raise RuntimeError(f"hub_dir not found: {hub_dir}")

        self.hub_dir = hub_dir

        # get tasks.json file content
        with open(self.hub_dir / 'hub-config' / 'tasks.json') as fp:
            tasks = json.load(fp)

        # check for predtimechart config file
        if not ptc_config_file.exists():
            raise RuntimeError(f"predtimechart config file not found: {ptc_config_file}")

        # load config settings from the predtimechart config file, first validating it
        with open(ptc_config_file) as fp:
            ptc_config = yaml.safe_load(fp)
            try:
                _validate_predtimechart_config(ptc_config, tasks)
            except ValidationError as ve:
                raise RuntimeError(f"invalid ptc_config_file. error='{ve}'")

            # validate hub predtimechart compatibility. must be done after `_validate_predtimechart_config()`
            with open(self.hub_dir / 'hub-config' / 'model-metadata-schema.json') as fp:
                model_metadata_schema = json.load(fp)
            _validate_hub_ptc_compatibility(ptc_config, tasks, model_metadata_schema)

        self.rounds_idx = ptc_config['rounds_idx']
        self.model_tasks_idx = ptc_config['model_tasks_idx']
        self.reference_date_col_name = ptc_config['reference_date_col_name']
        self.target_date_col_name = ptc_config['target_date_col_name']
        self.horizon_col_name = ptc_config['horizon_col_name']
        self.initial_checked_models = ptc_config['initial_checked_models']
        self.disclaimer = ptc_config.get('disclaimer')

        # set task_id_text. keys: task_ids, values: value-to-text dict. ex:
        # {'location': {'US': 'United States', '01': 'Alabama', ..., '78': 'Virgin Islands'},  ...}
        self.task_id_text = ptc_config.get('task_id_text')

        # set model_ids
        self.model_id_to_metadata = {}
        for model_metadata_file in (list((self.hub_dir / 'model-metadata').glob('*.yml')) +
                                    list((self.hub_dir / 'model-metadata').glob('*.yaml'))):
            with open(model_metadata_file) as fp:
                model_metadata = yaml.safe_load(fp)
                model_id = f"{model_metadata['team_abbr']}-{model_metadata['model_abbr']}"
                self.model_id_to_metadata[model_id] = model_metadata

        # set task_ids
        model_tasks_ele = tasks['rounds'][self.rounds_idx]['model_tasks'][self.model_tasks_idx]
        self.task_ids = sorted(model_tasks_ele['task_ids'].keys())

        # set target_data_file_name: either None (if the hub implements our new time-series target data standard) which
        # means to use the fixed data file location "target-data/time-series.csv"), or the file name to look for in the
        # "target-data" dir. use the function HubConfig.get_target_data_file_name() to access the actual file name
        self.target_data_file_name = ptc_config.get('target_data_file_name')

        # set target info and viz_task_ids. recall: we require exactly one `target_metadata` entry, and only one
        # entry under its `target_keys`
        target_metadata = model_tasks_ele['target_metadata'][0]
        metadata_target_keys = target_metadata['target_keys']
        self.target_id = list(metadata_target_keys.values())[0]
        self.target_name = target_metadata['target_name']
        self.target_col_name = list(metadata_target_keys.keys())[0]
        self.viz_task_ids = sorted(set(self.task_ids) - {self.reference_date_col_name, self.target_date_col_name,
                                                         self.horizon_col_name, self.target_col_name})

        # set viz_task_id_to_vals
        self.viz_task_id_to_vals = defaultdict(list)  # union of task_ids required and optional fields. each can be null
        for viz_task_id in self.viz_task_ids:
            required_vals = model_tasks_ele['task_ids'][viz_task_id]['required']
            if required_vals:
                self.viz_task_id_to_vals[viz_task_id].extend(required_vals)

            optional_vals = model_tasks_ele['task_ids'][viz_task_id]['optional']
            if optional_vals:
                self.viz_task_id_to_vals[viz_task_id].extend(optional_vals)

        # set viz_task_ids_tuples, sorted by self.viz_task_ids
        viz_task_ids_values = [self.viz_task_id_to_vals[viz_task_id] for viz_task_id in self.viz_task_ids]
        self.viz_task_ids_tuples = list(itertools.product(*viz_task_ids_values))

        # set reference_dates
        ref_date_task_id = model_tasks_ele['task_ids'][self.reference_date_col_name]
        self.reference_dates = (ref_date_task_id['required'] if ref_date_task_id['required'] else []) + \
                               (ref_date_task_id['optional'] if ref_date_task_id['optional'] else [])


    def model_output_file_for_ref_date(self, model_id: str, reference_date: str) -> Optional[Path]:
        """
        Returns a Path to the model output file corresponding to `model_id` and `reference_date`. Returns None if none
        found.
        """
        poss_output_files = [self.hub_dir / 'model-output' / model_id / f"{reference_date}-{model_id}.csv",
                             self.hub_dir / 'model-output' / model_id / f"{reference_date}-{model_id}.parquet"]
        for poss_output_file in poss_output_files:
            if poss_output_file.exists():
                return poss_output_file

        return None


    def get_available_ref_dates(self) -> dict[str, list[str]]:
        """
        Returns a list of reference_dates with at least one forecast file.
        """


        def get_sorted_values_or_first_config_ref_date(reference_dates: set[str]):
            if len(reference_dates) == 0:
                return [min(self.reference_dates)]
            else:
                return sorted(list(reference_dates))


        # loop over every (reference_date X model_id) combination
        target_id_to_ref_date = {self.target_id: set()}
        for reference_date in self.reference_dates:  # ex: ['2022-10-22', '2022-10-29', ...]
            for model_id in self.model_id_to_metadata:  # ex: ['Flusight-baseline', 'MOBS-GLEAM_FLUH', ...]
                model_output_file = self.model_output_file_for_ref_date(model_id, reference_date)
                if model_output_file:
                    if model_output_file.suffix == '.csv':
                        df = pd.read_csv(model_output_file, usecols=[self.target_col_name])
                    elif model_output_file.suffix in ['.parquet', '.pqt']:
                        df = pd.read_parquet(model_output_file, columns=[self.target_col_name])
                    else:
                        raise RuntimeError(f"unsupported model output file type: {model_output_file!r}. "
                                           f"Only .csv and .parquet are supported")

                    df = df.loc[df[self.target_col_name] == self.target_id, :]
                    if not df.empty:
                        target_id_to_ref_date[self.target_id].add(reference_date)

        return {target_id: get_sorted_values_or_first_config_ref_date(reference_dates)
                for target_id, reference_dates in target_id_to_ref_date.items()}


    def get_target_data_df(self):
        """
        Loads the target data csv file from the hub repo for now, file path for target data is hard coded to 'target-data'.
        Raises FileNotFoundError if target data file does not exist.
        """
        target_data_file_path = self.hub_dir / 'target-data' / self.get_target_data_file_name()
        try:
            # the override schema handles the 'US' location (the only location that doesn't parse as Int64)
            # todo xx hard-coded column names
            return pl.read_csv(target_data_file_path, schema_overrides={'location': pl.String,
                                                                        'value': pl.Float64,
                                                                        'observation': pl.Float64},
                               null_values=["NA"])
        except FileNotFoundError as error:
            raise FileNotFoundError(f"target data file not found. {target_data_file_path=}, {error=}")


    def get_target_data_file_name(self):
        """
        :return: the target data file name under the "target-data" dir to use
        """
        return self.target_data_file_name if self.target_data_file_name else 'time-series.csv'


def _validate_hub_ptc_compatibility(ptc_config: dict, tasks: dict, model_metadata_schema: dict):
    """
    Validates a hub's predtimechart compatibility as identified in README.MD > Assumptions/limitations .

    :param ptc_config: dict loaded from a 'predtimechart-config.yml' file
    :param tasks: dict loaded from a hub's 'tasks.json' file
    :param model_metadata_schema: dict loaded from a hub's 'model-metadata-schema.json' file
    :raises ValidationError: if `tasks` is incompatible with predtimechart
    """
    # get the round and model_task
    try:
        the_round = tasks['rounds'][ptc_config['rounds_idx']]
    except IndexError:
        raise ValidationError(f"rounds_idx IndexError: #rounds={len(tasks['rounds'])}, "
                              f"rounds_idx={ptc_config['rounds_idx']}")

    try:
        the_model_task = the_round['model_tasks'][ptc_config['model_tasks_idx']]
    except IndexError:
        raise ValidationError(f"model_tasks_idx IndexError: #model_tasks={len(the_round['model_tasks'])}, "
                              f"model_tasks_idx={ptc_config['model_tasks_idx']}")

    # validate: only `model_tasks` groups with `quantile` output_types will be considered
    output_type = the_model_task['output_type']
    if 'quantile' not in output_type:
        raise ValidationError(f"no quantile output_type found. found types: {list(output_type.keys())}")

    # validate: required quantile levels are present
    quantile_levels = output_type['quantile']['output_type_id']['required']
    req_quantile_levels = {0.025, 0.25, 0.5, 0.75, 0.975}
    if not req_quantile_levels <= set(quantile_levels):  # subset
        raise ValidationError(f"some quantile output_type_ids are missing. required={req_quantile_levels}, "
                              f"found={set(quantile_levels)}")

    # validate: in the specified `model_tasks` object within the specified `rounds` object, there is exactly one
    # `target_metadata` entry, and only one entry under its `target_keys`
    if len(the_model_task['target_metadata']) != 1:
        raise ValidationError(f"not exactly one target_metadata object: {len(the_model_task['target_metadata'])}")

    # validate: is_step_ahead
    if not the_model_task['target_metadata'][0]['is_step_ahead']:
        raise ValidationError("model_tasks entry's is_step_ahead must be true")

    # validate: model metadata must contain a boolean `designated_model` field
    if 'designated_model' not in model_metadata_schema['required']:
        raise ValidationError(f"'designated_model' not found in model metadata schema's 'required' section")


def _validate_predtimechart_config(ptc_config: dict, tasks: dict):
    """
    Validates `ptc_config` against the schema in ptc_schema.py.

    :param ptc_config: dict loaded from a 'predtimechart-config.yml' file
    :param tasks: dict loaded from a 'tasks.json' file
    :raises ValidationError: if `ptc_config` is not valid
    """
    # validate against the JSON Schema and then do additional validations
    validate(ptc_config, ptc_config_schema)

    # validate: `rounds_idx` and `model_tasks_idx`
    try:
        tasks['rounds'][ptc_config['rounds_idx']]
    except IndexError as ke:
        raise ValidationError(f"invalid rounds_idx={ptc_config['rounds_idx']}. {ke=}")

    the_round = tasks['rounds'][ptc_config['rounds_idx']]
    try:
        the_round['model_tasks'][ptc_config['model_tasks_idx']]
    except IndexError as ke:
        raise ValidationError(f"invalid model_tasks_idx={ptc_config['model_tasks_idx']}. {ke=}")

    the_model_task = the_round['model_tasks'][ptc_config['model_tasks_idx']]

    # validate: column names found in `task_ids`:
    present_col_names = the_model_task['task_ids'].keys()
    required_col_names = {ptc_config[req_col_name_config_key] for req_col_name_config_key in
                          ['reference_date_col_name', 'target_date_col_name', 'horizon_col_name']}
    if not required_col_names <= set(present_col_names):  # subset
        raise ValidationError(f"some required columns are missing. required={required_col_names}, "
                              f"found={set(present_col_names)}")
