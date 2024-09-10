from pathlib import Path

import pytest

from hub_predtimechart.hub_config import HubConfig


def test_hub_config_complex_forecast_hub():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir)

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
    assert hub_config.fetch_reference_dates == [
        "2022-10-22", "2022-10-29", "2022-11-05", "2022-11-12", "2022-11-19", "2022-11-26", "2022-12-03", "2022-12-10",
        "2022-12-17", "2022-12-24", "2022-12-31", "2023-01-07", "2023-01-14", "2023-01-21", "2023-01-28", "2023-02-04",
        "2023-02-11", "2023-02-18", "2023-02-25", "2023-03-04", "2023-03-11", "2023-03-18", "2023-03-25", "2023-04-01",
        "2023-04-08", "2023-04-15", "2023-04-22", "2023-04-29", "2023-05-06", "2023-05-13", "2023-05-20", "2023-05-27"]
    # todo xx others - see README.MD > Required hub configuration


def test_hub_config_complex_scenario_hub():
    hub_dir = Path('tests/hubs/example-complex-scenario-hub')
    hub_config = HubConfig(hub_dir)

    assert hub_config.hub_dir == hub_dir
    assert hub_config.rounds_idx == 1  # 'rounds'[1]
    assert hub_config.model_tasks_idx == 0  # 'rounds'[1]['model_tasks'][0]
    assert hub_config.reference_date_col_name == 'origin_date'
    assert hub_config.target_date_col_name is None  # NB: invalid for predtimechart, which requires this column
    assert hub_config.horizon_col_name == 'horizon'
    assert sorted(list(hub_config.model_id_to_metadata.keys())) == sorted(['HUBuni-simexamp', 'hubcomp-examp'])
    assert hub_config.task_ids == sorted(['horizon', 'location', 'origin_date', 'scenario_id', 'target'])
    assert hub_config.target_col_name == 'target'
    assert hub_config.target_col_name == 'target'
    assert hub_config.viz_task_ids == sorted(['location', 'scenario_id'])
    assert hub_config.fetch_target_id == 'inc death'
    assert hub_config.fetch_target_name == 'Incident deaths'
    assert hub_config.fetch_task_ids == {
        'location': ['US', '01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18',
                     '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34',
                     '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51',
                     '53', '54', '55', '56'],
        'scenario_id': ['A-2022-05-09', 'B-2022-05-09', 'C-2022-05-09', 'D-2022-05-09']}
    assert hub_config.fetch_task_ids_tuples == [
        ('US', 'A-2022-05-09'), ('US', 'B-2022-05-09'), ('US', 'C-2022-05-09'), ('US', 'D-2022-05-09'),
        ('01', 'A-2022-05-09'), ('01', 'B-2022-05-09'), ('01', 'C-2022-05-09'), ('01', 'D-2022-05-09'),
        ('02', 'A-2022-05-09'), ('02', 'B-2022-05-09'), ('02', 'C-2022-05-09'), ('02', 'D-2022-05-09'),
        ('04', 'A-2022-05-09'), ('04', 'B-2022-05-09'), ('04', 'C-2022-05-09'), ('04', 'D-2022-05-09'),
        ('05', 'A-2022-05-09'), ('05', 'B-2022-05-09'), ('05', 'C-2022-05-09'), ('05', 'D-2022-05-09'),
        ('06', 'A-2022-05-09'), ('06', 'B-2022-05-09'), ('06', 'C-2022-05-09'), ('06', 'D-2022-05-09'),
        ('08', 'A-2022-05-09'), ('08', 'B-2022-05-09'), ('08', 'C-2022-05-09'), ('08', 'D-2022-05-09'),
        ('09', 'A-2022-05-09'), ('09', 'B-2022-05-09'), ('09', 'C-2022-05-09'), ('09', 'D-2022-05-09'),
        ('10', 'A-2022-05-09'), ('10', 'B-2022-05-09'), ('10', 'C-2022-05-09'), ('10', 'D-2022-05-09'),
        ('11', 'A-2022-05-09'), ('11', 'B-2022-05-09'), ('11', 'C-2022-05-09'), ('11', 'D-2022-05-09'),
        ('12', 'A-2022-05-09'), ('12', 'B-2022-05-09'), ('12', 'C-2022-05-09'), ('12', 'D-2022-05-09'),
        ('13', 'A-2022-05-09'), ('13', 'B-2022-05-09'), ('13', 'C-2022-05-09'), ('13', 'D-2022-05-09'),
        ('15', 'A-2022-05-09'), ('15', 'B-2022-05-09'), ('15', 'C-2022-05-09'), ('15', 'D-2022-05-09'),
        ('16', 'A-2022-05-09'), ('16', 'B-2022-05-09'), ('16', 'C-2022-05-09'), ('16', 'D-2022-05-09'),
        ('17', 'A-2022-05-09'), ('17', 'B-2022-05-09'), ('17', 'C-2022-05-09'), ('17', 'D-2022-05-09'),
        ('18', 'A-2022-05-09'), ('18', 'B-2022-05-09'), ('18', 'C-2022-05-09'), ('18', 'D-2022-05-09'),
        ('19', 'A-2022-05-09'), ('19', 'B-2022-05-09'), ('19', 'C-2022-05-09'), ('19', 'D-2022-05-09'),
        ('20', 'A-2022-05-09'), ('20', 'B-2022-05-09'), ('20', 'C-2022-05-09'), ('20', 'D-2022-05-09'),
        ('21', 'A-2022-05-09'), ('21', 'B-2022-05-09'), ('21', 'C-2022-05-09'), ('21', 'D-2022-05-09'),
        ('22', 'A-2022-05-09'), ('22', 'B-2022-05-09'), ('22', 'C-2022-05-09'), ('22', 'D-2022-05-09'),
        ('23', 'A-2022-05-09'), ('23', 'B-2022-05-09'), ('23', 'C-2022-05-09'), ('23', 'D-2022-05-09'),
        ('24', 'A-2022-05-09'), ('24', 'B-2022-05-09'), ('24', 'C-2022-05-09'), ('24', 'D-2022-05-09'),
        ('25', 'A-2022-05-09'), ('25', 'B-2022-05-09'), ('25', 'C-2022-05-09'), ('25', 'D-2022-05-09'),
        ('26', 'A-2022-05-09'), ('26', 'B-2022-05-09'), ('26', 'C-2022-05-09'), ('26', 'D-2022-05-09'),
        ('27', 'A-2022-05-09'), ('27', 'B-2022-05-09'), ('27', 'C-2022-05-09'), ('27', 'D-2022-05-09'),
        ('28', 'A-2022-05-09'), ('28', 'B-2022-05-09'), ('28', 'C-2022-05-09'), ('28', 'D-2022-05-09'),
        ('29', 'A-2022-05-09'), ('29', 'B-2022-05-09'), ('29', 'C-2022-05-09'), ('29', 'D-2022-05-09'),
        ('30', 'A-2022-05-09'), ('30', 'B-2022-05-09'), ('30', 'C-2022-05-09'), ('30', 'D-2022-05-09'),
        ('31', 'A-2022-05-09'), ('31', 'B-2022-05-09'), ('31', 'C-2022-05-09'), ('31', 'D-2022-05-09'),
        ('32', 'A-2022-05-09'), ('32', 'B-2022-05-09'), ('32', 'C-2022-05-09'), ('32', 'D-2022-05-09'),
        ('33', 'A-2022-05-09'), ('33', 'B-2022-05-09'), ('33', 'C-2022-05-09'), ('33', 'D-2022-05-09'),
        ('34', 'A-2022-05-09'), ('34', 'B-2022-05-09'), ('34', 'C-2022-05-09'), ('34', 'D-2022-05-09'),
        ('35', 'A-2022-05-09'), ('35', 'B-2022-05-09'), ('35', 'C-2022-05-09'), ('35', 'D-2022-05-09'),
        ('36', 'A-2022-05-09'), ('36', 'B-2022-05-09'), ('36', 'C-2022-05-09'), ('36', 'D-2022-05-09'),
        ('37', 'A-2022-05-09'), ('37', 'B-2022-05-09'), ('37', 'C-2022-05-09'), ('37', 'D-2022-05-09'),
        ('38', 'A-2022-05-09'), ('38', 'B-2022-05-09'), ('38', 'C-2022-05-09'), ('38', 'D-2022-05-09'),
        ('39', 'A-2022-05-09'), ('39', 'B-2022-05-09'), ('39', 'C-2022-05-09'), ('39', 'D-2022-05-09'),
        ('40', 'A-2022-05-09'), ('40', 'B-2022-05-09'), ('40', 'C-2022-05-09'), ('40', 'D-2022-05-09'),
        ('41', 'A-2022-05-09'), ('41', 'B-2022-05-09'), ('41', 'C-2022-05-09'), ('41', 'D-2022-05-09'),
        ('42', 'A-2022-05-09'), ('42', 'B-2022-05-09'), ('42', 'C-2022-05-09'), ('42', 'D-2022-05-09'),
        ('44', 'A-2022-05-09'), ('44', 'B-2022-05-09'), ('44', 'C-2022-05-09'), ('44', 'D-2022-05-09'),
        ('45', 'A-2022-05-09'), ('45', 'B-2022-05-09'), ('45', 'C-2022-05-09'), ('45', 'D-2022-05-09'),
        ('46', 'A-2022-05-09'), ('46', 'B-2022-05-09'), ('46', 'C-2022-05-09'), ('46', 'D-2022-05-09'),
        ('47', 'A-2022-05-09'), ('47', 'B-2022-05-09'), ('47', 'C-2022-05-09'), ('47', 'D-2022-05-09'),
        ('48', 'A-2022-05-09'), ('48', 'B-2022-05-09'), ('48', 'C-2022-05-09'), ('48', 'D-2022-05-09'),
        ('49', 'A-2022-05-09'), ('49', 'B-2022-05-09'), ('49', 'C-2022-05-09'), ('49', 'D-2022-05-09'),
        ('50', 'A-2022-05-09'), ('50', 'B-2022-05-09'), ('50', 'C-2022-05-09'), ('50', 'D-2022-05-09'),
        ('51', 'A-2022-05-09'), ('51', 'B-2022-05-09'), ('51', 'C-2022-05-09'), ('51', 'D-2022-05-09'),
        ('53', 'A-2022-05-09'), ('53', 'B-2022-05-09'), ('53', 'C-2022-05-09'), ('53', 'D-2022-05-09'),
        ('54', 'A-2022-05-09'), ('54', 'B-2022-05-09'), ('54', 'C-2022-05-09'), ('54', 'D-2022-05-09'),
        ('55', 'A-2022-05-09'), ('55', 'B-2022-05-09'), ('55', 'C-2022-05-09'), ('55', 'D-2022-05-09'),
        ('56', 'A-2022-05-09'), ('56', 'B-2022-05-09'), ('56', 'C-2022-05-09'), ('56', 'D-2022-05-09')
    ]
    assert hub_config.fetch_reference_dates == ['2022-06-05']


def test_model_output_file_for_ref_date():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir)

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


def test_hub_dir_does_not_exist():
    with pytest.raises(RuntimeError) as excinfo:
        hub_dir = Path('tests/hubs/example-complex-forecast-hub')
        HubConfig(hub_dir / 'bad-dir')
    assert "hub_dir not found" in str(excinfo.value)


def test_predtimechart_config_file_does_not_exist():
    with pytest.raises(RuntimeError) as excinfo:
        hub_dir = Path('tests/hubs/no-ptc-config-hub')
        HubConfig(hub_dir)
    assert "predtimechart config file not found" in str(excinfo.value)


@pytest.mark.skip(reason="todo")
def test_predtimechart_config_file_validity():
    # test predtimechart-config.yml validity. maybe use JSON Schema?
    pass


@pytest.mark.skip(reason="todo")
def test_hub_dir_ptc_compatibility():
    # constraints: see README.MD > Assumptions/limitations
    with pytest.raises(RuntimeError) as excinfo:
        hub_dir = Path('tests/hubs/example-complex-scenario-hub')
        HubConfig(hub_dir)
    assert "hub is incompatible with predtimechart" in str(excinfo.value)

    with pytest.raises(RuntimeError) as excinfo:
        hub_dir = Path('tests/hubs/no-ptc-config-hub')
        HubConfig(hub_dir)
    assert "hub is incompatible with predtimechart" in str(excinfo.value)
