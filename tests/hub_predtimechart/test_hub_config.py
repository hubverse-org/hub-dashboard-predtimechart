import copy
import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from jsonschema.exceptions import ValidationError

from hub_predtimechart.hub_config import HubConfig, _validate_predtimechart_config, _validate_hub_ptc_compatibility


def test_hub_config_complex_forecast_hub():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    assert hub_config.hub_dir == hub_dir
    assert hub_config.rounds_idx == 0  # 'rounds'[0]
    assert hub_config.model_tasks_idx == 2  # 'rounds'[0]['model_tasks'][2]
    assert hub_config.reference_date_col_name == 'reference_date'
    assert hub_config.target_date_col_name == 'target_end_date'
    assert hub_config.horizon_col_name == 'horizon'
    assert hub_config.initial_checked_models == ['Flusight-baseline']
    assert hub_config.disclaimer == "Most forecasts have failed to reliably predict rapid changes in the trends of reported cases and hospitalizations. Due to this limitation, they should not be relied upon for decisions about the possibility or timing of rapid changes in trends."
    assert (sorted(list(hub_config.model_id_to_metadata.keys())) ==
            sorted(['Flusight-baseline', 'MOBS-GLEAM_FLUH', 'PSI-DICE']))
    assert hub_config.task_ids == sorted(['reference_date', 'target', 'horizon', 'location', 'target_end_date'])
    assert hub_config.target_col_name == 'target'
    assert hub_config.viz_task_ids == sorted(['location'])
    assert hub_config.fetch_target_id == 'wk inc flu hosp'
    assert hub_config.fetch_target_name == 'incident influenza hospitalizations'
    assert hub_config.fetch_task_ids == {
        'location': ["US", "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17", "18",
                     "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34",
                     "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48", "49", "50", "51",
                     "53", "54", "55", "56", "72"]}
    assert hub_config.fetch_task_ids_tuples == [
        ("US",), ("01",), ("02",), ("04",), ("05",), ("06",), ("08",), ("09",), ("10",), ("11",), ("12",), ("13",),
        ("15",), ("16",), ("17",), ("18",), ("19",), ("20",), ("21",), ("22",), ("23",), ("24",), ("25",), ("26",),
        ("27",), ("28",), ("29",), ("30",), ("31",), ("32",), ("33",), ("34",), ("35",), ("36",), ("37",), ("38",),
        ("39",), ("40",), ("41",), ("42",), ("44",), ("45",), ("46",), ("47",), ("48",), ("49",), ("50",), ("51",),
        ("53",), ("54",), ("55",), ("56",), ("72",)]
    assert hub_config.reference_dates == [
        "2022-10-22", "2022-10-29", "2022-11-05", "2022-11-12", "2022-11-19", "2022-11-26", "2022-12-03", "2022-12-10",
        "2022-12-17", "2022-12-24", "2022-12-31", "2023-01-07", "2023-01-14", "2023-01-21", "2023-01-28", "2023-02-04",
        "2023-02-11", "2023-02-18", "2023-02-25", "2023-03-04", "2023-03-11", "2023-03-18", "2023-03-25", "2023-04-01",
        "2023-04-08", "2023-04-15", "2023-04-22", "2023-04-29", "2023-05-06", "2023-05-13", "2023-05-20", "2023-05-27"]

def test_hub_config_complex_forecast_hub_no_disclaimer():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir, Path('tests/configs/example-complex-no-disclaimer.yml'))
    assert hub_config.disclaimer == None


def test_model_output_file_for_ref_date():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    # case: exists
    file = hub_config.model_output_file_for_ref_date('Flusight-baseline', "2022-10-22")
    assert isinstance(file, Path)
    assert file.exists()
    assert file.name == '2022-10-22-Flusight-baseline.csv'

    # case: does not exist: bad model name, ok reference_date
    file = hub_config.model_output_file_for_ref_date('bad-model', "2022-10-22")
    assert file is None

    # case: does not exist: ok name, bad reference_date
    file = hub_config.model_output_file_for_ref_date('Flusight-baseline', "1999-10-22")
    assert file is None


def test_hub_dir_existence():
    with pytest.raises(RuntimeError, match="hub_dir not found"):
        hub_dir = Path('tests/hubs/example-complex-forecast-hub')
        HubConfig(hub_dir / 'nonexistent-dir', None)


def test_predtimechart_config_file_existence():
    with pytest.raises(RuntimeError, match="predtimechart config file not found"):
        hub_dir = Path('tests/hubs/no-ptc-config-hub')
        HubConfig(hub_dir, hub_dir / 'nonexistent-file.yml')


def test__validate_predtimechart_config():
    """
    tests predtimechart-config.yml validitiy. NB: for valid cases: example-complex-forecast-hub is valid and is
    checked elsewhere
    """
    # test a known invalid hub
    with pytest.raises(RuntimeError, match="invalid ptc_config_file"):
        hub_dir = Path('tests/hubs/invalid-ptc-config-hub')
        HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')

    # test that HubConfig() calls `_validate_predtimechart_config()`, and then test against that function directly
    with patch('hub_predtimechart.hub_config._validate_predtimechart_config') as validate_mock:
        hub_dir = Path('tests/hubs/example-complex-forecast-hub')
        HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
        validate_mock.assert_called_once()

    # get a valid config file to work with and then invalidate it in a few different ways to make sure ptc_schema.py is
    # in play
    with open('tests/hubs/example-complex-forecast-hub/hub-config/predtimechart-config.yml') as fp:
        ecfh_ptc_config = yaml.safe_load(fp)

    # case: missing property
    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    del ecfh_ptc_config_copy['rounds_idx']
    with pytest.raises(ValidationError, match="'rounds_idx' is a required property"):
        _validate_predtimechart_config(ecfh_ptc_config_copy, {})  # tasks not necessary

    # case: empty string property
    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['reference_date_col_name'] = ''
    with pytest.raises(ValidationError, match="\'\' should be non-empty"):
        _validate_predtimechart_config(ecfh_ptc_config_copy, {})  # tasks not necessary

    # case: rounds_idx or model_tasks_idx is out of range
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    with open(hub_dir / 'hub-config' / 'tasks.json') as fp:
        ecfh_tasks = json.load(fp)
    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['rounds_idx'] = 1  # there is only one `rounds`, so this is invalid
    with pytest.raises(ValidationError, match="invalid rounds_idx"):
        _validate_predtimechart_config(ecfh_ptc_config_copy, ecfh_tasks)

    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['model_tasks_idx'] = 3  # there are only three `model_tasks` within round 0, so this is invalid
    with pytest.raises(ValidationError, match="invalid model_tasks_idx"):
        _validate_predtimechart_config(ecfh_ptc_config_copy, ecfh_tasks)

    # case: column not found
    for nonexistent_col_name in ['reference_date_col_name', 'target_date_col_name', 'horizon_col_name']:
        ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
        ecfh_ptc_config_copy[nonexistent_col_name] = 'nonexistent_column'
        with pytest.raises(ValidationError, match=f"some required columns are missing"):
            _validate_predtimechart_config(ecfh_ptc_config_copy, ecfh_tasks)


def test__validate_hub_ptc_compatibility():
    """
    Tests the constraints identified in README.MD > Assumptions/limitations .
    """

    # test that HubConfig() calls `_validate_hub_ptc_compatibility()`, and then test against that function directly
    with patch('hub_predtimechart.hub_config._validate_hub_ptc_compatibility') as validate_mock:
        hub_dir = Path('tests/hubs/example-complex-forecast-hub')
        HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
        validate_mock.assert_called_once()

    # case: model_tasks_idx does not have a quantile output_type
    with open('tests/hubs/example-complex-forecast-hub/hub-config/predtimechart-config.yml') as fp:
        ecfh_ptc_config = yaml.safe_load(fp)
    with open(Path('tests/hubs/example-complex-scenario-hub') / 'hub-config' / 'tasks.json') as fp:
        ecsh_tasks = json.load(fp)

    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['rounds_idx'] = 2
    ecfh_ptc_config_copy['model_tasks_idx'] = 0  # 2|0 only has "sample" "output_type"
    with pytest.raises(ValidationError, match="no quantile output_type found"):
        _validate_hub_ptc_compatibility(ecfh_ptc_config_copy, ecsh_tasks, {})

    # case: not all quantile levels present (0.025, 0.25, 0.5, 0.75, 0.975)
    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['rounds_idx'] = 1
    ecfh_ptc_config_copy['model_tasks_idx'] = 0  # 1|0 xx

    ecsh_tasks_copy = copy.deepcopy(ecsh_tasks)
    ecsh_round = ecsh_tasks_copy['rounds'][ecfh_ptc_config_copy['rounds_idx']]
    ecsh_model_task = ecsh_round['model_tasks'][ecfh_ptc_config_copy['model_tasks_idx']]
    ecsh_model_task['output_type']['quantile']['output_type_id']['required'] = [0.025, 0.25, 0.75, 0.975]  # no 0.5
    with pytest.raises(ValidationError, match="some quantile output_type_ids are missing"):
        _validate_hub_ptc_compatibility(ecfh_ptc_config_copy, ecsh_tasks_copy, {})

    # case: model metadata must contain a boolean `designated_model` field
    with open(Path('tests/hubs/example-complex-forecast-hub') / 'hub-config' / 'tasks.json') as fp:
        ecfh_tasks = json.load(fp)
    with pytest.raises(ValidationError, match="'designated_model' not found in model metadata schema"):
        _validate_hub_ptc_compatibility(ecfh_ptc_config, ecfh_tasks, {'required': []})

    # case: two or more `target_metadata` objects have different `target_keys` keys
    ecfh_tasks_copy = copy.deepcopy(ecfh_tasks)
    ecfh_round = ecfh_tasks_copy['rounds'][ecfh_ptc_config['rounds_idx']]
    ecfh_model_task = ecfh_round['model_tasks'][ecfh_ptc_config['model_tasks_idx']]
    first_target_metadata_obj = ecfh_model_task['target_metadata'][0]
    first_target_metadata_obj_copy = copy.deepcopy(first_target_metadata_obj)
    first_target_metadata_obj_copy['target_keys'] = {"target2": "wk inc flu hosp"}  # dup of orig, which has 'target'
    ecfh_model_task['target_metadata'].append(first_target_metadata_obj_copy)
    with open(Path('tests/hubs/example-complex-forecast-hub') / 'hub-config' / 'model-metadata-schema.json') as fp:
        ecfh_model_metadata_schema = json.load(fp)
    with pytest.raises(ValidationError, match="more than one unique `target_metadata` key found"):
        _validate_hub_ptc_compatibility(ecfh_ptc_config, ecfh_tasks_copy, ecfh_model_metadata_schema)
