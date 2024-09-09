import json
from pathlib import Path

from hub_predtimechart.generate_options import ptc_options_for_hub
import pytest
from hub_predtimechart.hub_config import HubConfig


@pytest.mark.skip(reason="todo")
def test_generate_options():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir=hub_dir)
    with open('tests/expected/example-complex-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    for exp_field in [
        # 'target_variables', 'initial_target_var',
        # 'task_ids', 'initial_task_ids',
        'intervals', 'initial_interval',
        # 'available_as_ofs', 'initial_as_of', 'current_date',
        'models', 'initial_checked_models', 'disclaimer', 'initial_xaxis_range'
    ]:
        assert exp_field in act_options
        assert act_options[exp_field] == exp_options[exp_field]

    assert act_options == exp_options
