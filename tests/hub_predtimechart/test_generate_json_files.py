import json
import tempfile
from pathlib import Path

from hub_predtimechart.app.generate_json_files import _generate_json_files, _generate_options_file
from hub_predtimechart.hub_config import HubConfig


def test_generate_json_files(tmp_path):
    """
    An integration test of `generate_json_files.py`'s `_generate_json_files()`.
    """
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    output_dir = tmp_path
    json_files = _generate_json_files(hub_config, output_dir)
    assert set(json_files) == {output_dir / 'wk-inc-flu-hosp_US_2022-10-22.json',
                               output_dir / 'wk-inc-flu-hosp_01_2022-10-22.json',
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


def test_generate_options_file(tmp_path):
    """
    An integration test of `generate_json_files.py`'s `_generate_options_file()`.
    """
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    ptc_options = tmp_path / 'ptc_options'
    _generate_options_file(hub_config, ptc_options)
    with open(ptc_options) as act_options_fp, \
            open('tests/expected/example-complex-forecast-hub/predtimechart-options.json') as exp_options_fp:
        act_options = json.load(act_options_fp)
        exp_options = json.load(exp_options_fp)
        assert act_options == exp_options
