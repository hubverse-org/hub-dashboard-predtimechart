import json
from pathlib import Path

from hub_predtimechart.generate_options import ptc_options_for_hub
from hub_predtimechart.hub_config import HubConfig


def test_generate_options_complex_forecast_hub():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
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
    hub_config = HubConfig(hub_dir, Path('tests/configs/example-complex-no-disclaimer.yml'))
    with open('tests/expected/example-complex-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert 'disclaimer' not in act_options


def test_generate_options_flusight_forecast_hub():
    hub_dir = Path('tests/hubs/FluSight-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with open('tests/expected/FluSight-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert act_options == exp_options


def test_generate_options_task_id_text_covid19_forecast_hub():
    hub_dir = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfig(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with open('tests/expected/covid19-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert act_options == exp_options
