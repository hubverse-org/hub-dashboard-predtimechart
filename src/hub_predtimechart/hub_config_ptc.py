import itertools
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
import polars as pl
import yaml
from jsonschema import ValidationError, validate

from hub_predtimechart.hub_config import HubConfig
from hub_predtimechart.ptc_schema import ptc_config_schema


class HubConfigPtc(HubConfig):
    """
    A HubConfig subclass that adds various visualization-related variables from a hub.

    Instance variables:
    - rounds_idx: as loaded from `ptc_config_file`
    - reference_date_col_name: ""
    - horizon_col_name: ""
    - initial_checked_models: ""
    - disclaimer: ""
    - task_id_text: "". optional (None if not passed). keys: task_ids, values: value-to-text dict. ex:
        {'location': {'US': 'United States', '01': 'Alabama', ..., '78': 'Virgin Islands'},  ...}
    - target_data_file_name: either None (if the hub implements our new time-series target data standard) which means to
        use the fixed data file location "target-data/time-series.csv"), or the file name to look for in the
        "target-data" dir. use the function HubConfigPtc.get_target_data_file_name() to access the actual file name
    - model_id_to_metadata: maps model_ids (team_abbr + model_abbr) to metadata as loaded from files in the hub's
        'model-metadata' dir. functions both as a map to metadata and as an iterable of model_ids (keys)
    - model_tasks: a list of ModelTask instances that are applicable to predtimechart viz, i.e., is_step_ahead is true
        and 'quantile' is in output_type
    """


    def __init__(self, hub_dir: Path, ptc_config_file: Path):
        """
        :param hub_dir: as defined in HubConfig.__init__()
        :param ptc_config_file: location of `predtimechart-config.yml` (or other named) file that matches ptc_schema.py.
            this file specifies how to process `hub_dir` to get predtimechart output
        """
        super().__init__(hub_dir)

        if not ptc_config_file.exists():
            raise RuntimeError(f"predtimechart config file not found: {ptc_config_file}")

        # load config settings from the predtimechart config file, first validating it
        with open(ptc_config_file) as fp:
            ptc_config = yaml.safe_load(fp)
            try:
                _validate_predtimechart_config(ptc_config, self.tasks)
            except ValidationError as ve:
                raise RuntimeError(f"invalid ptc_config_file. error='{ve}'")

        self.rounds_idx: int = ptc_config['rounds_idx']
        self.reference_date_col_name: str = ptc_config['reference_date_col_name']
        self.target_date_col_name = ptc_config['target_date_col_name']
        self.horizon_col_name: str = ptc_config['horizon_col_name']
        self.initial_checked_models: str = ptc_config['initial_checked_models']
        self.disclaimer: str | None = ptc_config.get('disclaimer')  # optional
        self.task_id_text: dict | None = ptc_config.get('task_id_text')  # optional
        self.target_data_file_name: str | None = ptc_config.get('target_data_file_name')  # optional

        # set model_id_to_metadata
        self.model_id_to_metadata: dict[str, dict] = {}
        for model_metadata_file in (list((self.hub_dir / 'model-metadata').glob('*.yml')) +
                                    list((self.hub_dir / 'model-metadata').glob('*.yaml'))):
            with open(model_metadata_file) as fp:
                model_metadata = yaml.safe_load(fp)
                model_id = f"{model_metadata['team_abbr']}-{model_metadata['model_abbr']}"
                self.model_id_to_metadata[model_id] = model_metadata

        # set model_tasks. recall we support only one entry in target_metadata
        self.model_tasks = [ModelTask(self, model_task)
                            for model_task in self.tasks['rounds'][self.rounds_idx]['model_tasks']
                            if ('quantile' in model_task['output_type'])
                            and (model_task['target_metadata'][0]['is_step_ahead'])]

        # validate hub predtimechart compatibility. must be done after `_validate_predtimechart_config()`. also, we do
        # it here after I've finished initializing for convenience
        _validate_hub_ptc_compatibility(self)


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


    def get_target_data_df(self) -> pl.DataFrame:
        """
        Loads the target data csv file from the hub repo for now, file path for target data is hard coded to 'target-data'.
        Raises FileNotFoundError if target data file does not exist.
        """
        target_data_file_path = self.hub_dir / 'target-data' / self.get_target_data_file_name()
        try:
            # the override schema handles the 'US' location (the only location that doesn't parse as Int64)
            # todo hard-coded column names
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


def _validate_predtimechart_config(ptc_config: dict, tasks: dict):
    """
    Validates `ptc_config` against the schema in ptc_schema.py.

    :param ptc_config: dict loaded from a 'predtimechart-config.yml' file
    :param tasks: dict loaded from a 'tasks.json' file
    :raises ValidationError: if `ptc_config` is not valid
    """
    # validate against the JSON Schema and then do additional validations
    validate(ptc_config, ptc_config_schema)

    # validate: `rounds_idx`
    try:
        tasks['rounds'][ptc_config['rounds_idx']]
    except IndexError as ke:
        raise ValidationError(f"invalid rounds_idx: #rounds={len(tasks['rounds'])}, "
                              f"rounds_idx={ptc_config['rounds_idx']}")

    # validate: column names found in `task_ids`. NB: this assumes all model_task entries in the round have the same
    # task_ids
    the_round = tasks['rounds'][ptc_config['rounds_idx']]
    model_task_0 = the_round['model_tasks'][0]
    required_col_names = {ptc_config[req_col_name_config_key] for req_col_name_config_key in
                          ['reference_date_col_name', 'target_date_col_name', 'horizon_col_name']}
    present_col_names = model_task_0['task_ids'].keys()
    if not required_col_names <= set(present_col_names):  # subset
        raise ValidationError(f"some required columns are missing. required={required_col_names}, "
                              f"found={set(present_col_names)}")


def _validate_hub_ptc_compatibility(hub_config_ptc: HubConfigPtc):
    """
    Validates a hub's predtimechart compatibility as identified in README.MD > Assumptions/limitations .

    :param hub_config_ptc: the HubConfigPtc being validated
    """
    # validate: must have at least one applicable model_task entry - is_step_ahead is true and 'quantile' is in
    # output_type (these were filtered in caller)
    if not hub_config_ptc.model_tasks:
        raise ValidationError(f"no applicable model_task entries were found")

    # validate: model metadata must contain a boolean `designated_model` field
    if 'designated_model' not in hub_config_ptc.model_metadata_schema['required']:
        raise ValidationError(f"'designated_model' not found in model metadata schema's 'required' section")

    # validate: all model_task entries have the same task_ids. frozenset lets us make a set of sets
    task_ids_all_model_tasks = [frozenset(model_task.task['task_ids'].keys()) for model_task in hub_config_ptc.model_tasks]
    if len(set(task_ids_all_model_tasks)) != 1:
        raise ValidationError(f"not all model_task entries have the same task_ids")

    for model_task in hub_config_ptc.model_tasks:
        # validate: required quantile levels are present
        quantile_levels = model_task.task['output_type']['quantile']['output_type_id']['required']
        req_quantile_levels = {0.025, 0.25, 0.5, 0.75, 0.975}
        if not req_quantile_levels <= set(quantile_levels):  # subset
            raise ValidationError(f"some quantile output_type_ids are missing. required={req_quantile_levels}, "
                                  f"found={set(quantile_levels)}")

        # validate: must be exactly one `target_metadata` entry, and only one entry under its `target_keys`
        if len(model_task.task['target_metadata']) != 1:
            raise ValidationError(f"not exactly one target_metadata object: {len(model_task.task['target_metadata'])}")

        target_metadata_target_keys = model_task.task['target_metadata'][0]['target_keys']
        if len(target_metadata_target_keys) != 1:
            raise ValidationError(f"not exactly one target_metadata target_keys entry: {target_metadata_target_keys}")


#
# ModelTask
#

@dataclass
class ModelTask:
    """
    A HubConfigPtc helper class that represents a tasks.json "model_tasks" entry under a round.

    Instance variables:
    - hub_config_ptc: the HubConfigPtc creating this instance
    - task: a "model_tasks" entry as loaded from tasks.json. has three keys per the hubverse standard: 'task_ids',
        'output_type', and 'target_metadata'
    - viz_target_id: target_metadata.target_keys[0].value
    - viz_target_name: target_metadata.target_name
    - viz_target_col_name: target_metadata.target_keys[0].key
    - viz_task_ids: column names from model_tasks.task_ids.keys minus reference_date col, target_date col, horizon col,
        and target col
    - viz_task_id_to_vals: dict that maps task_ids (model_tasks.task_ids.keys) to a union of required and optional
        fields
    - viz_task_ids_tuples: the product of all viz task_id values
    - viz_reference_dates: union of required and optional reference_dates from the reference column. sorted by date
    """
    hub_config_ptc: HubConfigPtc = field(repr=False)
    task: dict = field(repr=False)
    viz_target_id: str = field(init=False, repr=False)
    viz_target_name: str = field(init=False, repr=False)
    viz_target_col_name: str = field(init=False, repr=False)
    viz_task_ids: list = field(init=False, repr=False)
    viz_task_id_to_vals: dict = field(init=False, repr=False)
    viz_task_ids_tuples: list = field(init=False, repr=False)
    viz_reference_dates: list = field(init=False, repr=False)


    def __post_init__(self):
        # recall: we require exactly one `target_metadata` entry, and only one entry under its `target_keys`
        target_metadata = self.task['target_metadata'][0]
        self.viz_target_id = list(target_metadata['target_keys'].values())[0]
        self.viz_target_name = target_metadata['target_name']
        self.viz_target_col_name = list(target_metadata['target_keys'].keys())[0]

        task_ids_keys = sorted(self.task['task_ids'].keys())
        self.viz_task_ids = sorted(set(task_ids_keys) - {self.hub_config_ptc.reference_date_col_name,
                                                         self.hub_config_ptc.target_date_col_name,
                                                         self.hub_config_ptc.horizon_col_name,
                                                         self.viz_target_col_name})

        # set viz_task_id_to_vals
        self.viz_task_id_to_vals = defaultdict(list)  # union of task_ids required and optional fields. each can be null
        for viz_task_id in self.viz_task_ids:
            required_vals = self.task['task_ids'][viz_task_id]['required']
            if required_vals:
                self.viz_task_id_to_vals[viz_task_id].extend(required_vals)

            optional_vals = self.task['task_ids'][viz_task_id]['optional']
            if optional_vals:
                self.viz_task_id_to_vals[viz_task_id].extend(optional_vals)

        # set viz_task_ids_tuples, sorted by self.viz_task_ids
        viz_task_ids_values = [self.viz_task_id_to_vals[viz_task_id] for viz_task_id in self.viz_task_ids]
        self.viz_task_ids_tuples = list(itertools.product(*viz_task_ids_values))

        # set viz_reference_dates
        ref_date_task_id = self.task['task_ids'][self.hub_config_ptc.reference_date_col_name]
        self.viz_reference_dates = sorted((ref_date_task_id['required'] if ref_date_task_id['required'] else []) +
                                          (ref_date_task_id['optional'] if ref_date_task_id['optional'] else []))


    def get_available_ref_dates(self) -> list[str]:
        """
        Returns a list of viz_reference_dates with at least one forecast file.
        """


        def get_sorted_values_or_first_config_ref_date(reference_dates: set[str]):
            if len(reference_dates) == 0:
                return [min(self.viz_reference_dates)]
            else:
                return sorted(list(reference_dates))


        # loop over every (reference_date X model_id) combination
        reference_dates = set()
        for reference_date in self.viz_reference_dates:  # ex: ['2022-10-22', '2022-10-29', ...]
            for model_id in self.hub_config_ptc.model_id_to_metadata:  # ex: 'Flusight-baseline'
                model_output_file = self.hub_config_ptc.model_output_file_for_ref_date(model_id, reference_date)
                if model_output_file:
                    if model_output_file.suffix == '.csv':
                        df = pd.read_csv(model_output_file, usecols=[self.viz_target_col_name])
                    elif model_output_file.suffix in ['.parquet', '.pqt']:
                        df = pd.read_parquet(model_output_file, columns=[self.viz_target_col_name])
                    else:
                        raise RuntimeError(f"unsupported model output file type: {model_output_file!r}. "
                                           f"Only .csv and .parquet are supported")

                    df = df.loc[df[self.viz_target_col_name] == self.viz_target_id, :]
                    if not df.empty:
                        reference_dates.add(reference_date)

        return get_sorted_values_or_first_config_ref_date(reference_dates)
