import json
import tempfile
from pathlib import Path

from hub_predtimechart.app.generate_json_files import _main
from hub_predtimechart.hub_config import HubConfig


def test_generate_json_files():
    """
    An integration test of generate_json_files.py .
    """
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir=hub_dir)
    with tempfile.TemporaryDirectory() as temp_dir_name:
        output_dir = Path(temp_dir_name)
        json_files = _main(hub_config, output_dir)
        assert set(json_files) == {output_dir / 'wk-inc-flu-hosp_US_2022-10-22.json',
                                   output_dir / 'wk-inc-flu-hosp_01_2022-10-22.json'}

        with open('tests/expected/example-complex-forecast-hub/forecasts/wk-inc-flu-hosp_US_2022-10-22.json') as exp_fp, \
                open(output_dir / 'wk-inc-flu-hosp_US_2022-10-22.json') as act_fp:
            exp_data = json.load(exp_fp)
            act_data = json.load(act_fp)
            assert act_data == exp_data

        with open('tests/expected/example-complex-forecast-hub/forecasts/wk-inc-flu-hosp_01_2022-10-22.json') as exp_fp, \
                open(output_dir / 'wk-inc-flu-hosp_01_2022-10-22.json') as act_fp:
            exp_data = json.load(exp_fp)
            act_data = json.load(act_fp)
            assert act_data == exp_data
