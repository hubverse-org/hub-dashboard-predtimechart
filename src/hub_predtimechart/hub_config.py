import itertools
import json
from collections import defaultdict
from pathlib import Path
from typing import Optional

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
        self.disclaimer = ptc_config['disclaimer']

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

        # set viz_task_ids and fetch_targets. recall: we assume there is only one target_metadata entry, only one
        # entry under its `target_keys`
        target_metadata = model_tasks_ele['target_metadata'][0]
        metadata_target_keys = target_metadata['target_keys']
        self.target_col_name = list(metadata_target_keys.keys())[0]
        self.viz_task_ids = sorted(set(self.task_ids) - {self.reference_date_col_name, self.target_date_col_name,
                                                         self.horizon_col_name, self.target_col_name})
        self.fetch_target_id = list(metadata_target_keys.values())[0]
        self.fetch_target_name = target_metadata['target_name']

        # set fetch_task_ids
        self.fetch_task_ids = defaultdict(list)  # union of task_ids required and optional fields. each can be null
        for viz_task_id in self.viz_task_ids:
            required_vals = model_tasks_ele['task_ids'][viz_task_id]['required']
            if required_vals:
                self.fetch_task_ids[viz_task_id].extend(required_vals)

            optional_vals = model_tasks_ele['task_ids'][viz_task_id]['optional']
            if optional_vals:
                self.fetch_task_ids[viz_task_id].extend(optional_vals)

        # set fetch_task_ids_tuples, sorted by self.viz_task_ids
        viz_task_ids_values = [self.fetch_task_ids[viz_task_id] for viz_task_id in self.viz_task_ids]
        self.fetch_task_ids_tuples = list(itertools.product(*viz_task_ids_values))

        # set fetch_reference_dates
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


def _validate_hub_ptc_compatibility(ptc_config: dict, tasks: dict, model_metadata_schema: dict):
    """
    Validates a hub's predtimechart compatibility as identified in README.MD > Assumptions/limitations .

    :param ptc_config: dict loaded from a 'predtimechart-config.yml' file
    :param tasks: dict loaded from a hub's 'tasks.json' file
    :param model_metadata_schema: dict loaded from a hub's 'model-metadata-schema.json' file
    :raises ValidationError: if `tasks` is incompatible with predtimechart
    """
    # get the round and model_task
    the_round = tasks['rounds'][ptc_config['rounds_idx']]
    the_model_task = the_round['model_tasks'][ptc_config['model_tasks_idx']]

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

    # validate: model metadata must contain a boolean `designated_model` field
    if 'designated_model' not in model_metadata_schema['required']:
        raise ValidationError(f"'designated_model' not found in model metadata schema's 'required' section")

    # validate: in the specified `model_tasks` object within the specified `rounds` object, all objects in the
    # `target_metadata` list must have the same single key in the `target_keys` object
    target_keys_keys = set()
    for target_metadata_obj in the_model_task['target_metadata']:
        target_keys_keys.update(target_metadata_obj['target_keys'].keys())
    if len(target_keys_keys) != 1:
        raise ValidationError(f"more than one unique `target_metadata` key found: {target_keys_keys}")


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
