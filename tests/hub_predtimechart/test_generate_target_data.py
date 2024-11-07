import json
from datetime import date
from pathlib import Path

from freezegun import freeze_time
import polars as pl

from hub_predtimechart.app.generate_json_files import json_file_name
from hub_predtimechart.generate_target_data import target_data_for_FluSight
from hub_predtimechart.app.generate_target_json_files_FluSight import reference_date_from_today


def test_generate_target_data_flusight_forecast_hub():
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


@freeze_time("2024-10-24")
def test_reference_date_from_today():
    # Test dates are Sunday, Thursday, and Saturday.
    # For all of these, the expected reference date is the same: the Saturday
    exp_reference_date = date.fromisoformat("2024-10-26")
    for reference_date_str in ["2024-10-20", "2024-10-24", "2024-10-26"]:
        act_reference_date = reference_date_from_today(date.fromisoformat(reference_date_str))
        assert act_reference_date == exp_reference_date

    # test that now is used if no parameter passed
    # in this case, we expect that reference_date_from_today uses now,
    # which is set to "2024-10-24" via the freeze_time decorator.
    act_reference_date = reference_date_from_today()
    assert act_reference_date == exp_reference_date
