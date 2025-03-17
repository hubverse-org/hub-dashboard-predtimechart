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
    assert hub_config.target_data_file_name == 'covid-hospital-admissions.csv'
    assert hub_config.target_col_name == 'target'
    assert hub_config.viz_task_ids == sorted(['location'])
    assert hub_config.target_id == 'wk inc flu hosp'
    assert hub_config.target_name == 'incident influenza hospitalizations'
    assert hub_config.viz_task_id_to_vals == {
        'location': ["US", "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17", "18",
                     "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34",
                     "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48", "49", "50", "51",
                     "53", "54", "55", "56", "72"]}
    assert hub_config.viz_task_ids_tuples == [
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
    assert hub_config.disclaimer is None


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


def test_get_available_ref_dates():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    act_as_ofs = hub_config.get_available_ref_dates()
    exp_as_ofs = {'wk inc flu hosp': ['2022-10-22', '2022-11-19', '2022-12-17']}
    assert act_as_ofs == exp_as_ofs


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
    with open(Path('tests/hubs/example-complex-forecast-hub') / 'hub-config' / 'tasks.json') as fp:
        ecfh_tasks = json.load(fp)
    with open(Path('tests/hubs/example-complex-scenario-hub') / 'hub-config' / 'tasks.json') as fp:
        ecsh_tasks = json.load(fp)
    with open(Path('tests/hubs/FluSight-forecast-hub') / 'hub-config' / 'tasks.json') as fp:
        fsfh_tasks = json.load(fp)

    # case: 'rounds_idx' and 'model_tasks_idx' IndexError
    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['rounds_idx'] = 1  # only one round
    with pytest.raises(ValidationError, match="rounds_idx IndexError"):
        _validate_hub_ptc_compatibility(ecfh_ptc_config_copy, ecfh_tasks, {})

    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['model_tasks_idx'] = 3  # only three model_tasks
    with pytest.raises(ValidationError, match="model_tasks_idx IndexError"):
        _validate_hub_ptc_compatibility(ecfh_ptc_config_copy, ecfh_tasks, {})

    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['rounds_idx'] = 2
    ecfh_ptc_config_copy['model_tasks_idx'] = 0  # 2|0 only has "sample" "output_type"
    with pytest.raises(ValidationError, match="no quantile output_type found"):
        _validate_hub_ptc_compatibility(ecfh_ptc_config_copy, ecsh_tasks, {})

    # case: not all quantile levels present (0.025, 0.25, 0.5, 0.75, 0.975)
    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['rounds_idx'] = 1
    ecfh_ptc_config_copy['model_tasks_idx'] = 0  # 1|0

    ecsh_tasks_copy = copy.deepcopy(ecsh_tasks)
    ecsh_round = ecsh_tasks_copy['rounds'][ecfh_ptc_config_copy['rounds_idx']]
    ecsh_model_task = ecsh_round['model_tasks'][ecfh_ptc_config_copy['model_tasks_idx']]
    ecsh_model_task['output_type']['quantile']['output_type_id']['required'] = [0.025, 0.25, 0.75, 0.975]  # no 0.5
    with pytest.raises(ValidationError, match="some quantile output_type_ids are missing"):
        _validate_hub_ptc_compatibility(ecfh_ptc_config_copy, ecsh_tasks_copy, {})

    # case: model_tasks_idx is not is_step_ahead
    fsfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    fsfh_ptc_config_copy['model_tasks_idx'] = 2  # 0|2 is "is_step_ahead": false
    with pytest.raises(ValidationError, match="model_tasks entry's is_step_ahead must be true"):
        _validate_hub_ptc_compatibility(fsfh_ptc_config_copy, fsfh_tasks, {})

    # case: model metadata must contain a boolean `designated_model` field
    with pytest.raises(ValidationError, match="'designated_model' not found in model metadata schema"):
        _validate_hub_ptc_compatibility(ecfh_ptc_config, ecfh_tasks, {'required': []})

    # case: not exactly one "target_metadata" object
    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['rounds_idx'] = 0
    ecfh_ptc_config_copy['model_tasks_idx'] = 0
    with pytest.raises(ValidationError, match="not exactly one target_metadata object"):
        _validate_hub_ptc_compatibility(ecfh_ptc_config_copy, ecsh_tasks, {'required': []})


def test_task_id_text_covid19_forecast_hub():
    hub_dir = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    assert hub_config.task_id_text == {
        'location': {
            'US': 'United States', '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas',
            '06': 'California', '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware', '11': 'District of Columbia',
            '12': 'Florida', '13': 'Georgia', '15': 'Hawaii', '16': 'Idaho', '17': 'Illinois', '18': 'Indiana',
            '19': 'Iowa', '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana', '23': 'Maine', '24': 'Maryland',
            '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota', '28': 'Mississippi', '29': 'Missouri',
            '30': 'Montana', '31': 'Nebraska', '32': 'Nevada', '33': 'New Hampshire', '34': 'New Jersey',
            '35': 'New Mexico', '36': 'New York', '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio',
            '40': 'Oklahoma', '41': 'Oregon', '42': 'Pennsylvania', '44': 'Rhode Island', '45': 'South Carolina',
            '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas', '49': 'Utah', '50': 'Vermont', '51': 'Virginia',
            '53': 'Washington', '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming', '60': 'American Samoa',
            '66': 'Guam', '69': 'Northern Mariana Islands', '72': 'Puerto Rico', '74': 'U.S. Minor Outlying Islands',
            '78': 'Virgin Islands'}
    }


def test_get_target_data_file_name():
    # hub that predates the new target data standard file name. specifies file name in hub_config.target_data_file_name
    hub_dir = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    assert hub_config.get_target_data_file_name() == 'covid-hospital-admissions.csv'

    # hub that predates the new target data standard file name. specifies file name in hub_config.target_data_file_name
    hub_dir = Path('tests/hubs/FluSight-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    assert hub_config.get_target_data_file_name() == 'target-hospital-admissions.csv'

    # hub that uses the new target data standard file name: "target-data/time-series.csv"
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    assert hub_config.get_target_data_file_name() == 'time-series.csv'
