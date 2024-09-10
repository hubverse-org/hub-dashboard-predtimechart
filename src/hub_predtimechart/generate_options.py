from hub_predtimechart.hub_config import HubConfig


def ptc_options_for_hub(hub_config: HubConfig):
    """
    Returns predtimechart options dict for `hub_config` - see https://github.com/reichlab/predtimechart?tab=readme-ov-file#options-object .

    :param hub_config: a HubConfig
    """
    options = {}

    # set `target_variables` and `initial_target_var`. recall that we currently only support one target
    options['target_variables'] = [{'value': hub_config.fetch_target_id,
                                    'text': hub_config.fetch_target_name,
                                    'plot_text': hub_config.fetch_target_name}]
    options['initial_target_var'] = options['target_variables'][0]['value']

    # set `task_ids` and `initial_task_ids`
    options['task_ids'] = {}
    for task_id, task_values in hub_config.fetch_task_ids.items():
        options['task_ids'][task_id] = [{'value': task_value, 'text': task_value } for task_value in task_values]
    options['initial_task_ids'] = {task_id: task_values[0]['value'] for task_id, task_values in options['task_ids'].items()}

    # set `intervals` and `initial_interval`
    options['intervals'] = ["0%", "50%", "95%"]
    options['initial_interval'] = options['intervals'][-1]

    # set `available_as_ofs`, `initial_as_of`, and `current_date`
    options['available_as_ofs'] = {task_id: hub_config.fetch_reference_dates for task_id in hub_config.fetch_task_ids}
    options['initial_as_of'] = hub_config.fetch_reference_dates[-1]
    options['current_date'] = hub_config.fetch_reference_dates[-1]

    # set `models` and `initial_checked_models`
    options['models'] = []
    for model_id, metadata in hub_config.model_id_to_metadata.items():
        if metadata['designated_model']:
            options['models'].append(model_id)
    options['models'].sort()
    options['initial_checked_models'] = hub_config.initial_checked_models

    # set `disclaimer`
    options['disclaimer'] = hub_config.disclaimer

    # set `initial_xaxis_range`
    options['initial_xaxis_range'] = None

    return options
