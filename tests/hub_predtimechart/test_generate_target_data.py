import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from freezegun import freeze_time

from hub_predtimechart.app.generate_target_json_files import reference_date_from_today, get_target_data_df, \
    ptc_target_data


def test_generate_target_data_flusight_forecast_hub():
    hub_dir = Path('tests/hubs/FluSight-forecast-hub')
    target_data_df = get_target_data_df(hub_dir, 'target-hospital-admissions.csv')
    for loc in ['US', '01']:
        task_ids_tuple = (loc,)
        with open(f'tests/expected/FluSight-forecast-hub/target/wk-inc-flu-hosp_{loc}.json') as fp:
            exp_data = json.load(fp)
            act_data = ptc_target_data(target_data_df, task_ids_tuple)
            assert act_data == exp_data


def test_generate_target_data_covid19_forecast_hub():
    hub_dir = Path('tests/hubs/covid19-forecast-hub')
    target_data_df = get_target_data_df(hub_dir, 'covid-hospital-admissions.csv')
    for loc in ['US', '01']:
        task_ids_tuple = (loc,)
        with open(f'tests/expected/covid19-forecast-hub/target/wk-inc-flu-hosp_{loc}.json') as fp:
            exp_data = json.load(fp)
            act_data = ptc_target_data(target_data_df, task_ids_tuple)
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


def test_get_target_data_df_error_cases():
    hub_dir = Path('tests/hubs/FluSight-forecast-hub')
    act_target_data_df = get_target_data_df(hub_dir, 'target-hospital-admissions-no-na.csv')
    assert act_target_data_df["value"].dtype == pl.datatypes.Int64
    assert act_target_data_df["value"].to_list() == [3, 16, 30, 106, 151, 23, 64, 8, 2, 266]

    act_target_data_df = get_target_data_df(hub_dir, 'target-hospital-admissions-yes-na.csv')
    assert act_target_data_df["value"].dtype == pl.datatypes.Int64
    assert act_target_data_df["value"].to_list() == [3, 16, None, 106, 151, 23, 64, 8, 2, 266]

    # case: file not found
    with pytest.raises(FileNotFoundError, match="target_data_filename was missing"):
        get_target_data_df(hub_dir, None)

    with pytest.raises(FileNotFoundError, match="target data file not found"):
        get_target_data_df(hub_dir, 'non-existent-file.csv')
