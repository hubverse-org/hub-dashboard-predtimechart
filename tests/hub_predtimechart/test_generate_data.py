import json
from pathlib import Path

import pandas as pd

from hub_predtimechart.app.generate_json_files import json_file_name
from hub_predtimechart.generate_data import forecast_data_for_model_df
from hub_predtimechart.hub_config_ptc import HubConfigPtc


def test_json_file_name():
    # case: isForecast, targetKey, taskIDs, referenceDate: True, 'wk inc flu hosp', {location: US}, '2022-10-22'
    assert json_file_name('wk inc flu hosp', ('US',), '2022-10-22') == 'wk-inc-flu-hosp_US_2022-10-22.json'

    # case: isForecast, targetKey, taskIDs, referenceDate: True, 'wk inc flu hosp', {location: 01}, '2022-10-22'
    assert json_file_name('wk inc flu hosp', ('01',), '2022-10-22') == 'wk-inc-flu-hosp_01_2022-10-22.json'


def test_forecast_data_for_model_df_complex_forecast_hub_US():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    with open('tests/expected/example-complex-forecast-hub/forecasts/wk-inc-flu-hosp_US_2022-10-22.json') as fp:
        exp_data = json.load(fp)
    target = 'wk inc flu hosp'
    task_ids_tuple = ('US',)

    # case: Flusight-baseline
    model_output_file = hub_dir / 'model-output/Flusight-baseline/2022-10-22-Flusight-baseline.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), target, task_ids_tuple)
    assert act_data == exp_data['Flusight-baseline']

    # case: MOBS-GLEAM_FLUH
    model_output_file = hub_dir / 'model-output/MOBS-GLEAM_FLUH/2022-10-22-MOBS-GLEAM_FLUH.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), target, task_ids_tuple)
    assert act_data == exp_data['MOBS-GLEAM_FLUH']

    # case: PSI-DICE
    model_output_file = hub_dir / 'model-output/PSI-DICE/2022-10-22-PSI-DICE.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), target, task_ids_tuple)
    assert act_data == exp_data['PSI-DICE']


def test_forecast_data_for_model_df_complex_forecast_hub_01():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    with open('tests/expected/example-complex-forecast-hub/forecasts/wk-inc-flu-hosp_01_2022-10-22.json') as fp:
        exp_data = json.load(fp)
    target = 'wk inc flu hosp'
    task_ids_tuple = ('01',)

    # case: Flusight-baseline
    model_output_file = hub_dir / 'model-output/Flusight-baseline/2022-10-22-Flusight-baseline.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), target, task_ids_tuple)
    assert act_data == exp_data['Flusight-baseline']

    # case: MOBS-GLEAM_FLUH
    model_output_file = hub_dir / 'model-output/MOBS-GLEAM_FLUH/2022-10-22-MOBS-GLEAM_FLUH.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), target, task_ids_tuple)
    assert act_data == exp_data['MOBS-GLEAM_FLUH']

    # case: PSI-DICE
    model_output_file = hub_dir / 'model-output/PSI-DICE/2022-10-22-PSI-DICE.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), target, task_ids_tuple)
    assert act_data == exp_data['PSI-DICE']


def test_forecast_data_for_model_df_no_data():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    model_output_file = hub_dir / 'model-output/Flusight-baseline/2022-10-22-Flusight-baseline.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), 'wk inc flu hosp', ('01',))
    assert act_data != {}

    model_output_file = hub_dir / 'model-output/Flusight-baseline/2022-10-22-Flusight-baseline.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), 'wk inc flu hosp', ('02',))
    assert act_data == {}
