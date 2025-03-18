import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from freezegun import freeze_time

from hub_predtimechart.app.generate_target_json_files import reference_date_from_today, ptc_target_data, \
    _max_as_of_le__reference_date
from hub_predtimechart.hub_config_ptc import HubConfigPtc


def test_ptc_target_data_flusight_forecast_hub():
    hub_dir = Path('tests/hubs/FluSight-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    target_data_df = hub_config.get_target_data_df()
    for loc in ['US', '01']:
        task_ids_tuple = (loc,)
        with open(f'tests/expected/FluSight-forecast-hub/target/wk-inc-flu-hosp_{loc}.json') as fp:
            exp_data = json.load(fp)
            act_data = ptc_target_data(hub_config, target_data_df, task_ids_tuple, None)
            assert act_data == exp_data


def test_ptc_target_data_covid19_forecast_hub():
    hub_dir = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    target_data_df = hub_config.get_target_data_df()
    for loc in ['US', '01']:
        task_ids_tuple = (loc,)
        with open(f'tests/expected/covid19-forecast-hub/target/wk-inc-covid-hosp_{loc}.json') as fp:
            exp_data = json.load(fp)
            act_data = ptc_target_data(hub_config, target_data_df, task_ids_tuple, None)
            assert act_data == exp_data


def test_ptc_target_data_flu_metrocast():
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    target_data_df = hub_config.get_target_data_df()
    # 2025-02-25 is the newest as_of in tests/hubs/flu-metrocast time-series.csv. we use 2025-03-01 for
    # reference_date b/c that's the one right after 2025-02-25 (but we could have used any reference_date after
    # 2025-02-25)
    reference_date = '2025-03-01'
    for loc in ['Bronx', 'Manhattan']:
        task_ids_tuple = (loc,)
        with open(f'tests/expected/flu-metrocast/target/ILI-ED-visits_{loc}_{reference_date}.json') as fp:
            exp_data = json.load(fp)
            act_data = ptc_target_data(hub_config, target_data_df, task_ids_tuple, reference_date)
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
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    hub_config.target_data_file_name = 'target-hospital-admissions-no-na.csv'  # override
    act_target_data_df = hub_config.get_target_data_df()
    assert act_target_data_df['value'].dtype == pl.datatypes.Float64
    assert act_target_data_df['value'].to_list() == [3, 16, 30, 106, 151, 23, 64, 8, 2, 266]

    hub_config.target_data_file_name = 'target-hospital-admissions-yes-na.csv'  # override
    act_target_data_df = hub_config.get_target_data_df()
    assert act_target_data_df['value'].dtype == pl.datatypes.Float64
    assert act_target_data_df['value'].to_list() == [3, 16, None, 106, 151, 23, 64, 8, 2, 266]

    # case: file not found
    with pytest.raises(FileNotFoundError, match="target data file not found"):
        hub_config.target_data_file_name = 'non-existent-file.csv'  # override
        hub_config.get_target_data_df()


def test__max_as_of_le__reference_date():
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    target_data_df = hub_config.get_target_data_df()
    for ref_date, exp_max_as_of in [('2025-01-29', None), ('2025-02-12', '2025-02-12'), ('2025-02-13', '2025-02-12'),
                                    ('2025-02-26', '2025-02-25')]:
        act_max_as_of = _max_as_of_le__reference_date(target_data_df, ref_date)
        exp_max_as_of = date.fromisoformat(exp_max_as_of) if exp_max_as_of else exp_max_as_of
        assert act_max_as_of == exp_max_as_of
