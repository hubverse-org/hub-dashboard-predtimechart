import json
from pathlib import Path

import polars as pl

from hub_predtimechart.app.generate_json_files import json_file_name
from hub_predtimechart.generate_target_data import target_data_for_FluSight


def test_generate_target_data_for_FluSight():
    hub_dir = Path('tests/hubs/FluSight-forecast-hub')
    target_data_df = pl.read_csv(hub_dir / "target-data/target-hospital-admissions.csv")

    for loc in ['US', '01']:
        task_ids_tuple = (loc,)
        with open(f'tests/expected/FluSight-forecast-hub/target/wk-inc-flu-hosp_{loc}.json') as fp:
            exp_data = json.load(fp)

        # case: Flusight-baseline
        model_output_file = hub_dir / 'model-output/Flusight-baseline/2022-10-22-Flusight-baseline.csv'
        act_data = target_data_for_FluSight(target_data_df, task_ids_tuple)
        assert act_data == exp_data
