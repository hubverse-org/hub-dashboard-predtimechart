import json
from pathlib import Path


class HubConfig:
    """
    Provides convenient access to various parts of a hub's `tasks.json` file.

    Instance variables:
    - hub_dir: Path to a hub's root directory. see: https://hubverse.io/en/latest/user-guide/hub-structure.html
    - tasks: the hub's `tasks.json` contents
    - model_metadata_schema: the hub's `model-metadata-schema.json` contents
    """


    def __init__(self, hub_dir: Path):
        """
        :param hub_dir: Path to a hub's root directory. see: https://hubverse.io/en/latest/user-guide/hub-structure.html
        """

        self.hub_dir = hub_dir
        if not self.hub_dir.exists():
            raise RuntimeError(f"hub_dir not found: {self.hub_dir}")

        with open(self.hub_dir / 'hub-config' / 'tasks.json') as fp:
            self.tasks: dict = json.load(fp)

        with open(self.hub_dir / 'hub-config' / 'model-metadata-schema.json') as fp:
            self.model_metadata_schema: dict = json.load(fp)
