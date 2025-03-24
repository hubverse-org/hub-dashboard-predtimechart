from collections import defaultdict

import pandas as pd

from hub_predtimechart.hub_config_ptc import HubConfigPtc


def forecast_data_for_model_df(hub_config: HubConfigPtc, model_df: pd.DataFrame, target: str,
                               task_ids_tuple: tuple[str]):
    """
    Returns a dict for a single model in the forecast data format documented at https://github.com/reichlab/predtimechart?tab=readme-ov-file#fetchdata-forecasts-data-format .
    That is, looking at that example, this function returns the VALUE for a particular model, such as that of
    "Flusight-baseline" in tests/expected/example-complex-forecast-hub/forecasts/wk-inc-flu-hosp_US_2022-10-22.json :

    {
        "target_end_date": ["2022-10-22", "2022-10-29"],
        "q0.025": [1114, 882],
        "q0.25": [1542, 1443],
        "q0.5": [1724, 1724],
        "q0.75": [1906, 2006],
        "q0.975": [2334, 2562]
    }

    This means it's up to the caller to assemble outputs from individual models to form the final output that's saved to
    a JSON file.
    """
    # get the ModelTask corresponding to target. raise RuntimeError if not exactly one found
    model_tasks_for_target = [model_task for model_task in hub_config.model_tasks if model_task.viz_target_id == target]
    if len(model_tasks_for_target) != 1:
        raise RuntimeError(f"not exactly one ModelTask found for target='{target}'")

    model_task = model_tasks_for_target[0]

    # filter rows using a sequence of `query()` calls
    model_df = model_df.query(f"{model_task.viz_target_col_name} == '{target}'")

    for idx, viz_task_id in enumerate(model_task.viz_task_ids):  # gives us task_ids_tuple order
        # hub_config.viz_task_ids ex:  'location', 'scenario_id'
        # task_ids_tuple          ex: ('US',       'A-2022-05-09')
        viz_task_id_value = task_ids_tuple[idx]
        model_df = model_df.query(f"{viz_task_id} == '{viz_task_id_value}'")

    model_df = model_df.query("output_type == 'quantile'")

    # note that we include both strings and numbers b/c we can't depend on the output_type_id being one or the other
    quantile_levels = (0.025, 0.25, 0.5, 0.75, 0.975, '0.025', '0.25', '0.5', '0.75', '0.975')
    model_df = model_df.query(f"output_type_id in {quantile_levels}")

    # groupby target_end_date
    forecasts = defaultdict(list)
    for target_end_date, group in model_df.groupby(hub_config.target_date_col_name):
        quantile_df = group.sort_values(by=['output_type_id'])[['output_type_id', 'value']]
        forecasts['target_end_date'].append(target_end_date)
        for output_type_id, value in quantile_df.itertuples(index=False):
            forecasts[f"q{output_type_id}"].append(value)  # e.g., 'q0.025'

    return forecasts
