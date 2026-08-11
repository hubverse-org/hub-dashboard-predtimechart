import json
from pathlib import Path

import pandas as pd
import pytest

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


def test_forecast_data_for_model_df_flu_metrocast_bronx():
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    with open('tests/expected/flu-metrocast/forecasts/ILI-ED-visits_Bronx_2025-02-22.json') as fp:
        exp_data = json.load(fp)
    target = 'ILI ED visits'
    task_ids_tuple = ('Bronx',)

    # case: epiENGAGE-GBQR
    model_output_file = hub_dir / 'model-output/epiENGAGE-GBQR/2025-02-22-epiENGAGE-GBQR.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), target, task_ids_tuple)
    assert act_data == exp_data['epiENGAGE-GBQR']


def test_forecast_data_for_model_df_no_data():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    model_output_file = hub_dir / 'model-output/Flusight-baseline/2022-10-22-Flusight-baseline.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), 'wk inc flu hosp', ('01',))
    assert act_data != {}

    model_output_file = hub_dir / 'model-output/Flusight-baseline/2022-10-22-Flusight-baseline.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), 'wk inc flu hosp', ('02',))
    assert act_data == {}


#
# 80% interval (q0.1/q0.9) tests
#

def test_forecast_data_for_model_df_80_pct_quantiles():
    # the quantile levels backing the 80% interval are extracted when the model submitted them
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    model_output_file = hub_dir / 'model-output/Flusight-baseline/2022-10-22-Flusight-baseline.csv'
    act_data = forecast_data_for_model_df(hub_config, pd.read_csv(model_output_file), 'wk inc flu hosp', ('US',))
    assert list(act_data.keys()) == ['target_end_date', 'q0.025', 'q0.1', 'q0.25', 'q0.5', 'q0.75', 'q0.9', 'q0.975']
    assert act_data['q0.1'] == [1329, 1183]
    assert act_data['q0.9'] == [2119, 2266]


def test_forecast_data_for_model_df_no_80_pct_quantiles():
    # case: no backing forecast quantiles for the 80% interval. the model submitted only the required quantile levels,
    # so the output simply has no 'q0.1'/'q0.9' keys (predtimechart plots no interval, as with the "0%" option).
    # NB: Test-NumericOnly has exclusively numeric location codes, hence the `dtype` - see
    # `_generate_forecast_json_files()`
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    model_output_file = hub_dir / 'model-output/Test-NumericOnly/2022-10-22-Test-NumericOnly.csv'
    model_df = pd.read_csv(model_output_file, dtype={'location': str})
    act_data = forecast_data_for_model_df(hub_config, model_df, 'wk inc flu hosp', ('01',))
    assert list(act_data.keys()) == ['target_end_date', 'q0.025', 'q0.25', 'q0.5', 'q0.75', 'q0.975']
    assert 'q0.1' not in act_data
    assert 'q0.9' not in act_data


def test_forecast_data_for_model_df_partial_80_pct_quantiles():
    # case: the model submitted only one of the two quantile levels backing the 80% interval. we extract what's there
    # and leave it to predtimechart to skip an interval that's missing a bound
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    model_output_file = hub_dir / 'model-output/Flusight-baseline/2022-10-22-Flusight-baseline.csv'
    model_df = pd.read_csv(model_output_file)
    model_df = model_df.loc[model_df['output_type_id'] != 0.9, :]  # 0.1 remains
    act_data = forecast_data_for_model_df(hub_config, model_df, 'wk inc flu hosp', ('US',))
    assert 'q0.1' in act_data
    assert 'q0.9' not in act_data


def test_forecast_data_for_model_df_invalid_target():
    # test for the passed target not being present in any hub_config.model_tasks's viz_target_id
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with pytest.raises(RuntimeError, match="not exactly one ModelTask found for target"):
        forecast_data_for_model_df(hub_config, None, 'bad target', None)
