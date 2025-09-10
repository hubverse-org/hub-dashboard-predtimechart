import json
from pathlib import Path

import pytest

from hub_predtimechart.generate_options import ptc_options_for_hub, _host_owner_name
from hub_predtimechart.hub_config_ptc import HubConfigPtc


def test_generate_options_complex_forecast_hub():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with open('tests/expected/example-complex-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    for exp_field in ['target_variables', 'initial_target_var', 'task_ids', 'initial_task_ids', 'intervals',
                      'initial_interval', 'available_as_ofs', 'initial_as_of', 'current_date', 'models',
                      'initial_checked_models', 'disclaimer', 'initial_xaxis_range']:
        assert exp_field in act_options
        assert act_options[exp_field] == exp_options[exp_field]

    assert act_options == exp_options


def test_generate_options_complex_forecast_hub_no_disclaimer():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, Path('tests/configs/example-complex-no-disclaimer.yml'))
    with open('tests/expected/example-complex-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert 'disclaimer' not in act_options


def test_generate_options_flusight_forecast_hub():
    hub_dir = Path('tests/hubs/FluSight-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with open('tests/expected/FluSight-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert act_options == exp_options


def test_generate_options_flu_metrocast():
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with open('tests/expected/flu-metrocast/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert act_options == exp_options


def test_generate_options_task_id_text_covid19_forecast_hub():
    hub_dir = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with open('tests/expected/covid19-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert act_options == exp_options


def test_generate_options_initial_xaxis_range_covid19_forecast_hub():
    hub_dir = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config-xaxis.yml')
    with open('tests/expected/covid19-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    exp_options['initial_xaxis_range'] = hub_config.initial_xaxis_range
    act_options = ptc_options_for_hub(hub_config)
    assert act_options == exp_options


#
# model_urls tests
#

@pytest.mark.parametrize("hub_dir,exp_host_owner_name", [
    ('tests/hubs/covid19-forecast-hub',
     ('github', 'CDCgov', 'covid19-forecast-hub')),
    ('tests/hubs/example-complex-forecast-hub',
     ('github', 'Infectious-Disease-Modeling-Hubs', 'example-complex-forecast-hub')),
    ('tests/hubs/flu-metrocast',
     ('github', 'reichlab', 'flu-metrocast')),
    ('tests/hubs/FluSight-forecast-hub',
     ('github', 'cdcepi', 'FluSight-forecast-hub'))])
def test__host_owner_name(hub_dir, exp_host_owner_name):
    hub_dir = Path(hub_dir)
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    assert _host_owner_name(hub_config) == exp_host_owner_name


def test__host_owner_name_invalid_keys():
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    hub_config.admin['repository'] = {'bad-repo': 0}
    with pytest.raises(RuntimeError, match="invalid admin repository keys"):
        _host_owner_name(hub_config)
