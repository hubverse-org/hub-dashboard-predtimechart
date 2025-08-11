import copy
import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from jsonschema.exceptions import ValidationError

from hub_predtimechart.hub_config_ptc import HubConfigPtc, _validate_hub_ptc_compatibility, \
    _validate_predtimechart_config, ModelTask


def test_hub_config_complex_forecast_hub():
    hub_path = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    assert hub_config.rounds_idx == 0  # 'rounds'[0]
    assert hub_config.reference_date_col_name == 'reference_date'
    assert hub_config.target_date_col_name == 'target_end_date'
    assert hub_config.horizon_col_name == 'horizon'
    assert hub_config.initial_checked_models == ['Flusight-baseline']
    assert hub_config.disclaimer == "Most forecasts have failed to reliably predict rapid changes in the trends of reported cases and hospitalizations. Due to this limitation, they should not be relied upon for decisions about the possibility or timing of rapid changes in trends."
    assert (sorted(list(hub_config.model_id_to_metadata.keys())) ==
            sorted(['Flusight-baseline', 'MOBS-GLEAM_FLUH', 'PSI-DICE']))
    assert hub_config.target_data_file_name == 'covid-hospital-admissions.csv'

    model_task_0 = hub_config.model_tasks[0]  # only one
    assert model_task_0.viz_target_id == 'wk inc flu hosp'
    assert model_task_0.viz_target_name == 'incident influenza hospitalizations'
    assert model_task_0.viz_target_col_name == 'target'
    assert model_task_0.viz_task_ids == sorted(['location'])
    assert model_task_0.viz_task_id_to_vals == {
        'location': ["US", "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17", "18",
                     "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34",
                     "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48", "49", "50", "51",
                     "53", "54", "55", "56", "72"]}
    assert model_task_0.viz_task_ids_tuples == [
        ("US",), ("01",), ("02",), ("04",), ("05",), ("06",), ("08",), ("09",), ("10",), ("11",), ("12",), ("13",),
        ("15",), ("16",), ("17",), ("18",), ("19",), ("20",), ("21",), ("22",), ("23",), ("24",), ("25",), ("26",),
        ("27",), ("28",), ("29",), ("30",), ("31",), ("32",), ("33",), ("34",), ("35",), ("36",), ("37",), ("38",),
        ("39",), ("40",), ("41",), ("42",), ("44",), ("45",), ("46",), ("47",), ("48",), ("49",), ("50",), ("51",),
        ("53",), ("54",), ("55",), ("56",), ("72",)]
    assert model_task_0.viz_reference_dates == [
        "2022-10-22", "2022-10-29", "2022-11-05", "2022-11-12", "2022-11-19", "2022-11-26", "2022-12-03", "2022-12-10",
        "2022-12-17", "2022-12-24", "2022-12-31", "2023-01-07", "2023-01-14", "2023-01-21", "2023-01-28", "2023-02-04",
        "2023-02-11", "2023-02-18", "2023-02-25", "2023-03-04", "2023-03-11", "2023-03-18", "2023-03-25", "2023-04-01",
        "2023-04-08", "2023-04-15", "2023-04-22", "2023-04-29", "2023-05-06", "2023-05-13", "2023-05-20", "2023-05-27"]


def test_hub_config_complex_forecast_hub_no_disclaimer():
    hub_path = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_path, Path('tests/configs/example-complex-no-disclaimer.yml'))
    assert hub_config.disclaimer is None


def test_model_output_file_for_ref_date():
    hub_path = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')

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
    hub_path = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    model_task_0 = hub_config.model_tasks[0]
    act_as_ofs = model_task_0.get_available_ref_dates()
    exp_as_ofs = ['2022-10-22', '2022-11-19', '2022-12-17']
    assert act_as_ofs == exp_as_ofs


def test_predtimechart_config_file_existence():
    with pytest.raises(RuntimeError, match="predtimechart config file not found"):
        hub_path = Path('tests/hubs/no-ptc-config-hub')
        HubConfigPtc(hub_path, hub_path / 'nonexistent-file.yml')


def test__validate_predtimechart_config():
    """
    tests predtimechart-config.yml validitiy. NB: for valid cases: example-complex-forecast-hub is valid and is
    checked elsewhere
    """
    # test a known invalid hub
    with pytest.raises(RuntimeError, match="invalid ptc_config_file"):
        hub_path = Path('tests/hubs/invalid-ptc-config-hub')
        HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')  # missing `rounds_ids`

    # test that HubConfigPtc() calls `_validate_predtimechart_config()`, and then test against that function directly
    with patch('hub_predtimechart.hub_config_ptc._validate_predtimechart_config') as validate_mock:
        hub_path = Path('tests/hubs/example-complex-forecast-hub')
        HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
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

    # case: rounds_idx is out of range
    hub_path = Path('tests/hubs/example-complex-forecast-hub')
    with open(hub_path / 'hub-config' / 'tasks.json') as fp:
        ecfh_tasks = json.load(fp)
    ecfh_ptc_config_copy = copy.deepcopy(ecfh_ptc_config)
    ecfh_ptc_config_copy['rounds_idx'] = 1  # there is only one `rounds`, so this is invalid
    with pytest.raises(ValidationError, match="invalid rounds_idx"):
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

    # test that HubConfigPtc() calls `_validate_hub_ptc_compatibility()`, and then test against that function directly
    with patch('hub_predtimechart.hub_config_ptc._validate_hub_ptc_compatibility') as validate_mock:
        hub_path = Path('tests/hubs/example-complex-forecast-hub')
        HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
        validate_mock.assert_called_once()

    # case: no applicable model_task entries
    with pytest.raises(ValidationError, match="no applicable model_task entries were found"):
        hub_path = Path('tests/hubs/no-applicable-tasks-hub')
        hub_config_ptc = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
        _validate_hub_ptc_compatibility(hub_config_ptc)

    hub_path = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    with pytest.raises(ValidationError, match="some quantile output_type_ids are missing"):
        hub_config.model_tasks[0].task['output_type']['quantile']['output_type_id']['required'] = \
            [0.025, 0.25, 0.75, 0.975]  # no 0.5
        _validate_hub_ptc_compatibility(hub_config)

    # case: model metadata missing (case when hub-config/model-metadata-schema.json was not found)
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    hub_config.model_metadata_schema = None
    with pytest.raises(ValidationError, match="model metadata schema not found"):
        _validate_hub_ptc_compatibility(hub_config)

    # case: model metadata must contain a boolean `designated_model` field
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    hub_config.model_metadata_schema['required'].remove('designated_model')
    with pytest.raises(ValidationError, match="'designated_model' not found in model metadata schema"):
        _validate_hub_ptc_compatibility(hub_config)

    # case: must be exactly one `target_metadata` entry
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    target_metadata_list = hub_config.model_tasks[0].task['target_metadata']
    target_metadata_list.append(target_metadata_list[0])
    with pytest.raises(ValidationError, match="not exactly one target_metadata object"):
        _validate_hub_ptc_compatibility(hub_config)

    # case: only one entry under `target_metadata` entry's `target_keys`
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    target_keys = hub_config.model_tasks[0].task['target_metadata'][0]['target_keys']
    target_keys['second_target_key'] = 'second_target_key_value'
    with pytest.raises(ValidationError, match="not exactly one target_metadata target_keys entry"):
        _validate_hub_ptc_compatibility(hub_config)

    # case: all model_task entries have the same task_ids
    hub_path = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    hub_config.model_tasks[0].task['task_ids']['new_task'] = {'required': None, 'optional': None}
    with pytest.raises(ValidationError, match="not all model_task entries have the same task_ids"):
        _validate_hub_ptc_compatibility(hub_config)


def test_task_id_text_covid19_forecast_hub():
    hub_path = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
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
    hub_path = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    assert hub_config.get_target_data_file_name() == 'covid-hospital-admissions.csv'

    # hub that predates the new target data standard file name. specifies file name in hub_config.target_data_file_name
    hub_path = Path('tests/hubs/FluSight-forecast-hub')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    assert hub_config.get_target_data_file_name() == 'target-hospital-admissions.csv'

    # hub that uses the new target data standard file name: "target-data/time-series.csv"
    hub_path = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    assert hub_config.get_target_data_file_name() == 'time-series.csv'


def test_model_tasks_instance_list():
    for hub_path, exp_len in [('tests/hubs/covid19-forecast-hub', 1),
                             ('tests/hubs/example-complex-forecast-hub', 1),
                             ('tests/hubs/flu-metrocast', 2),
                             ('tests/hubs/FluSight-forecast-hub', 1)]:
        hub_path = Path(hub_path)
        hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
        assert len(hub_config.model_tasks) == exp_len
        assert all([isinstance(model_task, ModelTask) for model_task in hub_config.model_tasks])


def test_model_task_instance_flu_metrocast():
    hub_path = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    model_task_0 = hub_config.model_tasks[0]
    assert model_task_0.viz_target_id == 'ILI ED visits'
    assert model_task_0.viz_target_name == 'ED visits due to ILI'
    assert model_task_0.viz_target_col_name == 'target'
    assert model_task_0.viz_task_ids == ['location']
    assert model_task_0.viz_task_id_to_vals == {
        'location': ['NYC', 'Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island']
    }
    assert model_task_0.viz_task_ids_tuples == [
        ('NYC',), ('Bronx',), ('Brooklyn',), ('Manhattan',), ('Queens',), ('Staten Island',)
    ]
    assert model_task_0.viz_reference_dates == [
        '2025-01-25', '2025-02-01', '2025-02-08', '2025-02-15', '2025-02-22', '2025-03-01', '2025-03-08', '2025-03-15',
        '2025-03-22', '2025-03-29', '2025-04-05', '2025-04-12', '2025-04-19', '2025-04-26', '2025-05-03', '2025-05-10',
        '2025-05-17', '2025-05-24', '2025-05-31']


def test_model_task_viz_reference_dates_are_sorted():
    hub_path = Path('tests/hubs/unsorted-ref-dates')
    hub_config = HubConfigPtc(hub_path, hub_path / 'hub-config/predtimechart-config.yml')
    model_task_0 = hub_config.model_tasks[0]
    assert model_task_0.viz_reference_dates == [
        '2025-01-25', '2025-02-01', '2025-02-08', '2025-02-15', '2025-02-22', '2025-03-01', '2025-03-08', '2025-03-15',
        '2025-03-22', '2025-03-29', '2025-04-05', '2025-04-12', '2025-04-19', '2025-04-26', '2025-05-03', '2025-05-10',
        '2025-05-17', '2025-05-24', '2025-05-31']


def test_hub_path_is_a_path():
    # tests the invalid case of hub_path being a str. tests above handle the valid case of hub_path being a Path
    hub_path = Path('tests/hubs/flu-metrocast')
    with pytest.raises(TypeError, match='hub_path was not a Path'):
        HubConfigPtc(str(hub_path.absolute()), hub_path / 'hub-config/predtimechart-config.yml')
