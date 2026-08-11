import json
import shutil
from pathlib import Path

from hub_predtimechart.app.generate_json_files import _generate_forecast_json_files, _generate_options_file
from hub_predtimechart.hub_config_ptc import HubConfigPtc


def test_generate_forecast_json_files_ecfh(tmp_path):
    """
    An integration test of `generate_json_files.py`'s `_generate_json_files()` for example-complex-forecast-hub.
    """
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    output_dir = tmp_path
    json_files = _generate_forecast_json_files(hub_config, output_dir)
    assert set(json_files) == {output_dir / 'wk-inc-flu-hosp_US_2022-10-22.json',
                               output_dir / 'wk-inc-flu-hosp_01_2022-10-22.json',
                               output_dir / 'wk-inc-flu-hosp_02_2022-10-22.json',
                               output_dir / 'wk-inc-flu-hosp_US_2022-11-19.json',
                               output_dir / 'wk-inc-flu-hosp_01_2022-11-19.json',
                               output_dir / 'wk-inc-flu-hosp_US_2022-12-17.json',
                               output_dir / 'wk-inc-flu-hosp_01_2022-12-17.json'}
    for json_file in json_files:
        with open('tests/expected/example-complex-forecast-hub/forecasts/' + json_file.name) as exp_fp, \
                open(output_dir / json_file.name) as act_fp:
            exp_data = json.load(exp_fp)
            act_data = json.load(act_fp)
            assert act_data == exp_data



def test_generate_forecast_json_files_flu_metrocast(tmp_path):
    """
    An integration test of `generate_json_files.py`'s `_generate_json_files()` for flu-metrocast.
    """
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    output_dir = tmp_path
    act_json_files = _generate_forecast_json_files(hub_config, output_dir)
    assert set(act_json_files) == {output_dir / 'Flu-ED-visits-pct_Austin_2025-02-22.json',
                                   output_dir / 'Flu-ED-visits-pct_Austin_2025-03-01.json',
                                   output_dir / 'Flu-ED-visits-pct_Dallas_2025-02-22.json',
                                   output_dir / 'Flu-ED-visits-pct_Dallas_2025-03-01.json',
                                   output_dir / 'Flu-ED-visits-pct_El-Paso_2025-02-22.json',
                                   output_dir / 'Flu-ED-visits-pct_El-Paso_2025-03-01.json',
                                   output_dir / 'Flu-ED-visits-pct_Houston_2025-02-22.json',
                                   output_dir / 'Flu-ED-visits-pct_Houston_2025-03-01.json',
                                   output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-02-22.json',
                                   output_dir / 'Flu-ED-visits-pct_San-Antonio_2025-03-01.json',
                                   output_dir / 'ILI-ED-visits_Bronx_2025-02-01.json',
                                   output_dir / 'ILI-ED-visits_Bronx_2025-02-22.json',
                                   output_dir / 'ILI-ED-visits_Bronx_2025-03-01.json',
                                   output_dir / 'ILI-ED-visits_Brooklyn_2025-02-01.json',
                                   output_dir / 'ILI-ED-visits_Brooklyn_2025-02-22.json',
                                   output_dir / 'ILI-ED-visits_Brooklyn_2025-03-01.json',
                                   output_dir / 'ILI-ED-visits_Manhattan_2025-02-01.json',
                                   output_dir / 'ILI-ED-visits_Manhattan_2025-02-22.json',
                                   output_dir / 'ILI-ED-visits_Manhattan_2025-03-01.json',
                                   output_dir / 'ILI-ED-visits_NYC_2025-02-01.json',
                                   output_dir / 'ILI-ED-visits_NYC_2025-02-22.json',
                                   output_dir / 'ILI-ED-visits_NYC_2025-03-01.json',
                                   output_dir / 'ILI-ED-visits_Queens_2025-02-01.json',
                                   output_dir / 'ILI-ED-visits_Queens_2025-02-22.json',
                                   output_dir / 'ILI-ED-visits_Queens_2025-03-01.json',
                                   output_dir / 'ILI-ED-visits_Staten-Island_2025-02-01.json',
                                   output_dir / 'ILI-ED-visits_Staten-Island_2025-02-22.json',
                                   output_dir / 'ILI-ED-visits_Staten-Island_2025-03-01.json'}

    for json_file in act_json_files:
        with open('tests/expected/flu-metrocast/forecasts/' + json_file.name) as exp_fp, \
                open(output_dir / json_file.name) as act_fp:
            exp_data = json.load(exp_fp)
            act_data = json.load(act_fp)
            assert act_data == exp_data


def test_generate_forecast_json_files_80_pct_quantiles_ecfh(tmp_path):
    """
    An integration test for the 80% interval's backing quantile levels (0.1 and 0.9). example-complex-forecast-hub
    covers both cases within one hub: its 2022-10-22 and 2022-11-19 model outputs include those levels, while its
    2022-12-17 ones do not.
    """
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    output_dir = tmp_path
    _generate_forecast_json_files(hub_config, output_dir)

    # case: the backing quantiles are present
    with open(output_dir / 'wk-inc-flu-hosp_US_2022-10-22.json') as fp:
        act_data = json.load(fp)
    for model_id in ['Flusight-baseline', 'MOBS-GLEAM_FLUH', 'PSI-DICE']:
        assert 'q0.1' in act_data[model_id]
        assert 'q0.9' in act_data[model_id]

    # case: no backing quantiles. the files are still generated, just without the 'q0.1'/'q0.9' keys
    with open(output_dir / 'wk-inc-flu-hosp_US_2022-12-17.json') as fp:
        act_data = json.load(fp)
    assert set(act_data.keys()) == {'Flusight-baseline', 'MOBS-GLEAM_FLUH', 'PSI-DICE'}
    for model_id, model_data in act_data.items():
        assert 'q0.5' in model_data  # the required levels are unaffected
        assert 'q0.1' not in model_data
        assert 'q0.9' not in model_data

    # case: no backing quantiles for one model only (Test-NumericOnly submits just the required levels, while the
    # other models in the same file submit 0.1/0.9)
    with open(output_dir / 'wk-inc-flu-hosp_01_2022-10-22.json') as fp:
        act_data = json.load(fp)
    assert 'q0.1' in act_data['Flusight-baseline']
    assert 'q0.1' not in act_data['Test-NumericOnly']
    assert 'q0.9' not in act_data['Test-NumericOnly']


def test_generate_forecast_json_files_skip_files(tmp_path):
    """
    An integration test of `generate_json_files.py`'s `_generate_json_files()`.
    This validates that only new data will be generated
    """
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    output_dir = tmp_path

    # copy all but one prediction to the output directory
    shutil.copytree('tests/expected/example-complex-forecast-hub/forecasts/', output_dir, dirs_exist_ok=True)
    december = Path(output_dir / 'wk-inc-flu-hosp_US_2022-12-17.json')
    december.unlink()

    json_files = Path(output_dir).glob("*")
    assert set(json_files) == {output_dir / 'wk-inc-flu-hosp_US_2022-10-22.json',
                               output_dir / 'wk-inc-flu-hosp_01_2022-10-22.json',
                               output_dir / 'wk-inc-flu-hosp_02_2022-10-22.json',
                               output_dir / 'wk-inc-flu-hosp_US_2022-11-19.json',
                               output_dir / 'wk-inc-flu-hosp_01_2022-11-19.json',
                               output_dir / 'wk-inc-flu-hosp_01_2022-12-17.json'}

    # two JSON files should be generated because they are from the current round
    new_json_f = _generate_forecast_json_files(hub_config, output_dir)
    assert set(new_json_f) == {output_dir / 'wk-inc-flu-hosp_01_2022-12-17.json',
                               output_dir / 'wk-inc-flu-hosp_US_2022-12-17.json'}
    for json_file in set(json_files) | set(new_json_f):
        with open('tests/expected/example-complex-forecast-hub/forecasts/' + json_file.name) as exp_fp, \
                open(output_dir / json_file.name) as act_fp:
            exp_data = json.load(exp_fp)
            act_data = json.load(act_fp)
            assert act_data == exp_data


def test_generate_forecast_json_files_regenerate(tmp_path):
    """
    An integration test of `generate_json_files.py`'s `_generate_json_files()`.
    This validates that only new data will be generated
    """
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    output_dir = tmp_path

    # copy all but one prediction to the output directory
    shutil.copytree('tests/expected/example-complex-forecast-hub/forecasts/', output_dir, dirs_exist_ok=True)

    # all JSON files should be generated because they are from the current round
    json_files = _generate_forecast_json_files(hub_config, output_dir, True)
    assert set(json_files) == {output_dir / 'wk-inc-flu-hosp_US_2022-10-22.json',
                               output_dir / 'wk-inc-flu-hosp_01_2022-10-22.json',
                               output_dir / 'wk-inc-flu-hosp_02_2022-10-22.json',
                               output_dir / 'wk-inc-flu-hosp_US_2022-11-19.json',
                               output_dir / 'wk-inc-flu-hosp_01_2022-11-19.json',
                               output_dir / 'wk-inc-flu-hosp_US_2022-12-17.json',
                               output_dir / 'wk-inc-flu-hosp_01_2022-12-17.json'}


def test_generate_options_file(tmp_path):
    """
    An integration test of `generate_json_files.py`'s `_generate_options_file()`.
    """
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    ptc_options = tmp_path / 'ptc_options'
    _generate_options_file(hub_config, ptc_options)
    with open(ptc_options) as act_options_fp, \
            open('tests/expected/example-complex-forecast-hub/predtimechart-options.json') as exp_options_fp:
        act_options = json.load(act_options_fp)
        exp_options = json.load(exp_options_fp)
        assert act_options == exp_options
