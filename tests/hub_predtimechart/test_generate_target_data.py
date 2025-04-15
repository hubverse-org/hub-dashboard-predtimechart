import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from hub_predtimechart.app.generate_target_json_files import ptc_target_data, _generate_target_json_files, \
    _max_as_of_le_reference_date
from hub_predtimechart.hub_config_ptc import HubConfigPtc


def test_ptc_target_data_flusight_forecast_hub():
    hub_dir = Path('tests/hubs/FluSight-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    target_data_df = hub_config.get_target_data_df()
    for loc in ['US', '01']:
        task_ids_tuple = (loc,)
        with open(f'tests/expected/FluSight-forecast-hub/target/wk-inc-flu-hosp_{loc}.json') as fp:
            exp_data = json.load(fp)
            act_data = ptc_target_data(hub_config.model_tasks[0], target_data_df, task_ids_tuple, None)
            assert act_data == exp_data


def test_ptc_target_data_covid19_forecast_hub():
    hub_dir = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    target_data_df = hub_config.get_target_data_df()
    for loc in ['US', '01']:
        task_ids_tuple = (loc,)
        with open(f'tests/expected/covid19-forecast-hub/target/wk-inc-covid-hosp_{loc}.json') as fp:
            exp_data = json.load(fp)
            act_data = ptc_target_data(hub_config.model_tasks[0], target_data_df, task_ids_tuple, None)
            assert act_data == exp_data


def test_ptc_target_data_flu_metrocast():
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    target_data_df = hub_config.get_target_data_df()
    # re: reference_date: 2025-02-25 is the newest as_of in tests/hubs/flu-metrocast time-series.csv. we use 2025-03-01
    # for reference_date b/c that's the Saturday right after 2025-02-25, but we could have used any reference_date on or
    # after 2025-02-25
    reference_date = '2025-03-01'
    for loc in ['Bronx', 'Manhattan']:
        task_ids_tuple = (loc,)
        with open(f'tests/expected/flu-metrocast/targets/ILI-ED-visits_{loc}_2025-03-01.json') as fp:
            exp_data = json.load(fp)
            act_data = ptc_target_data(hub_config.model_tasks[0], target_data_df, task_ids_tuple, reference_date)
            assert act_data == exp_data

    for loc in ['Austin', 'Dallas']:
        task_ids_tuple = (loc,)
        with open(f'tests/expected/flu-metrocast/targets/Flu-ED-visits-pct_{loc}_2025-03-01.json') as fp:
            exp_data = json.load(fp)
            act_data = ptc_target_data(hub_config.model_tasks[1], target_data_df, task_ids_tuple, reference_date)
            assert act_data == exp_data


def test_ptc_target_data_flu_metrocast_no_data():
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    target_data_df = hub_config.get_target_data_df()

    # case: no max as_of <= reference_date. here there are no as_ofs <= '2025-01-29' (the first is '2025-03-01')
    assert ptc_target_data(hub_config.model_tasks[1], target_data_df, ('Austin',), '2025-01-29') is None

    # case: similar, but asking for a reference_date that has no as_ofs containing data for that target (the first TX
    # data starts '2025-02-12')
    assert ptc_target_data(hub_config.model_tasks[1], target_data_df, ('Austin',), '2025-02-11') is None


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


def test__max_as_of_le_reference_date_flu_metrocast():
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    target_data_df = hub_config.get_target_data_df()
    for ref_date, exp_max_as_of in [('2025-01-29', None), ('2025-02-12', '2025-02-12'), ('2025-02-13', '2025-02-12'),
                                    ('2025-02-26', '2025-02-25')]:
        act_max_as_of = _max_as_of_le_reference_date(target_data_df, ref_date)
        exp_max_as_of = date.fromisoformat(exp_max_as_of) if exp_max_as_of else exp_max_as_of
        assert act_max_as_of == exp_max_as_of


def test__generate_target_json_files_flu_metrocast(tmp_path):
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    target_data_df = hub_config.get_target_data_df()
    output_dir = tmp_path
    json_files = _generate_target_json_files(hub_config, target_data_df, output_dir)
    assert set(json_files) == {output_dir / 'Flu-ED-visits-pct_Austin_2025-02-15.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-02-22.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-03-01.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-03-08.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-03-15.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-03-22.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-03-29.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-04-05.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-04-12.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-04-19.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-04-26.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-05-03.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-05-10.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-05-17.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-05-24.json',
                               output_dir / 'Flu-ED-visits-pct_Austin_2025-05-31.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-02-15.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-02-22.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-03-01.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-03-08.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-03-15.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-03-22.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-03-29.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-04-05.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-04-12.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-04-19.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-04-26.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-05-03.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-05-10.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-05-17.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-05-24.json',
                               output_dir / 'Flu-ED-visits-pct_Dallas_2025-05-31.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-02-15.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-02-22.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-03-01.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-03-08.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-03-15.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-03-22.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-03-29.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-04-05.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-04-12.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-04-19.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-04-26.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-05-03.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-05-10.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-05-17.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-05-24.json',
                               output_dir / 'Flu-ED-visits-pct_El-Paso_2025-05-31.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-02-15.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-02-22.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-03-01.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-03-08.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-03-15.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-03-22.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-03-29.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-04-05.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-04-12.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-04-19.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-04-26.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-05-03.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-05-10.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-05-17.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-05-24.json',
                               output_dir / 'Flu-ED-visits-pct_Houston_2025-05-31.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-02-15.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-02-22.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-03-01.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-03-08.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-03-15.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-03-22.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-03-29.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-04-05.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-04-12.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-04-19.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-04-26.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-05-03.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-05-10.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-05-17.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-05-24.json',
                               output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-05-31.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-02-01.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-02-08.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-02-22.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-03-01.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-03-08.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-03-15.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-03-22.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-03-29.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-04-05.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-04-12.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-04-19.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-04-26.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-05-03.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-05-10.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-05-17.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-05-24.json',
                               output_dir / 'ILI-ED-visits_Bronx_2025-05-31.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-02-01.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-02-08.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-02-22.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-03-01.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-03-08.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-03-15.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-03-22.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-03-29.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-04-05.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-04-12.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-04-19.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-04-26.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-05-03.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-05-10.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-05-17.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-05-24.json',
                               output_dir / 'ILI-ED-visits_Brooklyn_2025-05-31.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-02-01.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-02-08.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-02-22.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-03-01.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-03-08.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-03-15.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-03-22.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-03-29.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-04-05.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-04-12.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-04-19.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-04-26.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-05-03.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-05-10.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-05-17.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-05-24.json',
                               output_dir / 'ILI-ED-visits_Manhattan_2025-05-31.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-02-01.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-02-08.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-02-22.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-03-01.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-03-08.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-03-15.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-03-22.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-03-29.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-04-05.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-04-12.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-04-19.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-04-26.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-05-03.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-05-10.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-05-17.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-05-24.json',
                               output_dir / 'ILI-ED-visits_NYC_2025-05-31.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-02-01.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-02-08.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-02-22.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-03-01.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-03-08.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-03-15.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-03-22.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-03-29.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-04-05.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-04-12.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-04-19.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-04-26.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-05-03.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-05-10.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-05-17.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-05-24.json',
                               output_dir / 'ILI-ED-visits_Queens_2025-05-31.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-02-01.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-02-08.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-02-22.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-03-01.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-03-08.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-03-15.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-03-22.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-03-29.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-04-05.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-04-12.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-04-19.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-04-26.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-05-03.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-05-10.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-05-17.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-05-24.json',
                               output_dir / 'ILI-ED-visits_Staten-Island_2025-05-31.json'}
    exp_json_files = {output_dir / 'ILI-ED-visits_Bronx_2025-03-01.json',
                      output_dir / 'ILI-ED-visits_Brooklyn_2025-03-08.json',
                      output_dir / 'ILI-ED-visits_Manhattan_2025-03-01.json',
                      output_dir / 'ILI-ED-visits_NYC_2025-03-08.json',
                      output_dir / 'ILI-ED-visits_Queens_2025-03-08.json',
                      output_dir / 'ILI-ED-visits_Staten-Island_2025-03-08.json',
                      output_dir / 'Flu-ED-visits-pct_Austin_2025-03-01.json',
                      output_dir / 'Flu-ED-visits-pct_Houston_2025-03-08.json',
                      output_dir / 'Flu-ED-visits-pct_Dallas_2025-03-01.json',
                      output_dir / 'Flu-ED-visits-pct_El-Paso_2025-03-08.json',
                      output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-03-08.json'}
    for json_file in exp_json_files:  # spot check contents of a few
        with open('tests/expected/flu-metrocast/targets/' + json_file.name) as exp_fp, \
                open(output_dir / json_file.name) as act_fp:
            exp_data = json.load(exp_fp)
            act_data = json.load(act_fp)
            assert act_data == exp_data
