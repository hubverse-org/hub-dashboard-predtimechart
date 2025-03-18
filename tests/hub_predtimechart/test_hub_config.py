from pathlib import Path

import pytest

from hub_predtimechart.hub_config_ptc import HubConfig


def test_hub_dir_existence():
    with pytest.raises(RuntimeError, match="hub_dir not found"):
        hub_dir = Path('tests/hubs/example-complex-forecast-hub')
        HubConfig(hub_dir / 'nonexistent-dir')


def test_hub_fields():
    hub_dir = Path('tests/hubs/example-complex-scenario-hub')
    hub_config = HubConfig(hub_dir)
    assert hub_config.hub_dir == hub_dir

    # spot-check tasks
    assert list(hub_config.tasks.keys()) == ['schema_version', 'rounds']
    assert len(hub_config.tasks['rounds']) == 4

    # spot-check model_metadata_schema
    assert (list(hub_config.model_metadata_schema.keys()) ==
            ['$schema', 'title', 'description', 'type', 'properties', 'additionalProperties', 'required'])
