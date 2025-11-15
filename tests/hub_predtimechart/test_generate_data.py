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


def test_forecast_data_for_model_df_invalid_target():
    # test for the passed target not being present in any hub_config.model_tasks's viz_target_id
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with pytest.raises(RuntimeError, match="not exactly one ModelTask found for target"):
        forecast_data_for_model_df(hub_config, None, 'bad target', None)


def test_forecast_data_dtype_inference_bug():
    """
    Test that demonstrates the dtype inference bug where models with only numeric location codes
    (e.g., "01", "02") have their location column read as int64 instead of object/string,
    causing filtering queries to fail and return empty data.

    This is a regression test for the issue where UMass-gbqr_spatial (and similar models)
    are silently excluded from visualizations because they don't forecast for "US" location,
    so pandas infers the location column as integer.
    """
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    # This model has ONLY numeric location codes: "01", "02"
    # Without the fix, pandas reads location as int64, causing query failure
    model_output_file = hub_dir / 'model-output/Test-NumericOnly/2022-10-22-Test-NumericOnly.csv'

    # Read the CSV the way the current code does (without dtype specification)
    model_df = pd.read_csv(model_output_file)

    # Verify the bug exists: location column is read as int64 instead of object
    assert model_df['location'].dtype == 'int64', \
        "Bug not reproduced: location should be int64 when file has only numeric codes"

    # This query will fail because it compares string '01' to integer 1
    target = 'wk inc flu hosp'
    task_ids_tuple = ('01',)
    act_data = forecast_data_for_model_df(hub_config, model_df, target, task_ids_tuple)

    # BUG: This returns empty dict instead of forecast data
    assert act_data == {}, \
        "Bug not reproduced: should return empty data due to dtype mismatch"

    # Now test with the correct dtype (what the fix should do)
    model_df_fixed = pd.read_csv(model_output_file, dtype={'location': str})
    assert model_df_fixed['location'].dtype == 'object', \
        "Fixed version should read location as string"

    act_data_fixed = forecast_data_for_model_df(hub_config, model_df_fixed, target, task_ids_tuple)

    # EXPECTED: Should return forecast data with correct structure
    assert act_data_fixed != {}, \
        "With correct dtype, should return non-empty forecast data"
    assert 'target_end_date' in act_data_fixed
    assert len(act_data_fixed['target_end_date']) > 0, \
        "Should have at least one target_end_date"
