from pathlib import Path

import pytest
from hub_predtimechart.hub_config import HubConfig
from hub_predtimechart.generate_options import ptc_options_for_hub


@pytest.mark.skip(reason="todo")
def test_generate_options():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfig(hub_dir=hub_dir)
    options = ptc_options_for_hub(hub_config)
    assert isinstance(options, dict)
