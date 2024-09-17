import itertools
import json
from collections import defaultdict
from pathlib import Path
from typing import Optional

import yaml


class HubConfig:
    """
    Provides various visualization-related variables that are parsed from various places in `hub_dir`.
    """


    def __init__(self, hub_dir: Path):
        """
        :param hub_dir: Path to a hub's root directory. see: https://hubverse.io/en/latest/user-guide/hub-structure.html
        """
        super().__init__()

        # check for hub_dir
        if not hub_dir.exists():
            raise RuntimeError(f"hub_dir not found: {hub_dir}")

        self.hub_dir = hub_dir

        # check for predtimechart config file
        ptc_config_file = self.hub_dir / 'hub-config' / 'predtimechart-config.yml'
        if not ptc_config_file.exists():
            raise RuntimeError(f"predtimechart config file not found: {ptc_config_file}")

        # load config settings from predtimechart config file
        with open(ptc_config_file, 'r') as fp:
            ptc_config = yaml.safe_load(fp)
            self.rounds_idx = ptc_config['rounds_idx']
            self.model_tasks_idx = ptc_config['model_tasks_idx']
            self.reference_date_col_name = ptc_config['reference_date_col_name']
            self.target_date_col_name = ptc_config['target_date_col_name']
            self.horizon_col_name = ptc_config['horizon_col_name']
            self.initial_checked_models = ptc_config['initial_checked_models']
            self.disclaimer = ptc_config['disclaimer']

        # set model_ids
        self.model_id_to_metadata = {}
        for model_metadata_file in (list((hub_dir / 'model-metadata').glob('*.yml')) +
                                    list((hub_dir / 'model-metadata').glob('*.yaml'))):
            with open(model_metadata_file, 'r') as fp:
                model_metadata = yaml.safe_load(fp)
                model_id = f"{model_metadata['team_abbr']}-{model_metadata['model_abbr']}"
                self.model_id_to_metadata[model_id] = model_metadata

        # set task_ids
        tasks_file = self.hub_dir / 'hub-config' / 'tasks.json'
        with open(tasks_file, 'r') as fp:
            tasks = json.load(fp)
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
