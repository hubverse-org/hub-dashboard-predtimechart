from collections import defaultdict

import polars as pl

from hub_predtimechart.hub_config import HubConfig


def target_data_for_FluSight(target_data_df: pl.DataFrame, task_ids_tuple: tuple[str]):
    """
    Returns a dict for a single reference date in the target data format documented at https://github.com/reichlab/predtimechart?tab=readme-ov-file#fetchdata-truth-data-format.
    That is, looking at that example, this function returns the date and value columns as in
    tests/expected/FluSight-forecast-hub/target/wk-inc-flu-hosp_US.json :

    {
      "date": ["2024-04-27", "2024-04-20", "..."],
      "y": [2337, 2860, "..."]
    }
    """
    loc = task_ids_tuple[0]
    target_data_loc = target_data_df.filter(pl.col("location") == loc).sort("date")
    target_data_ptc = {
        "date": target_data_loc["date"].to_list(),
        "y": target_data_loc["value"].to_list()
    }

    return target_data_ptc
