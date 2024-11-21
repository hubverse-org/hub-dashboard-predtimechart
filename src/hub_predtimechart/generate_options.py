from hub_predtimechart.hub_config import HubConfig


def ptc_options_for_hub(hub_config: HubConfig):
    """
    Returns predtimechart options dict for `hub_config` - see https://github.com/reichlab/predtimechart?tab=readme-ov-file#options-object .

    :param hub_config: a HubConfig
    """


    def task_text(task_id, task_value):
        if not isinstance(hub_config.task_id_text, dict):
            return task_value

        try:
            return hub_config.task_id_text[task_id][task_value]
        except KeyError:
            return task_value


    def get_max_ref_date_or_first_config_ref_date(reference_dates):
        if len(reference_dates) == 0:
            return min(hub_config.reference_dates)
        else:
            return max(reference_dates)


    # set `target_variables` and `initial_target_var`. recall that we currently only support one target
    options = {}
    options['target_variables'] = [{'value': hub_config.fetch_target_id,
                                    'text': hub_config.fetch_target_name,
                                    'plot_text': hub_config.fetch_target_name}]
    options['initial_target_var'] = options['target_variables'][0]['value']

    # set `task_ids` and `initial_task_ids`
    options['task_ids'] = {}
    for task_id, task_values in hub_config.fetch_task_ids.items():  # ex: {'location': ["US", "01", ...], ...}
        options['task_ids'][task_id] = [{'value': task_value, 'text': task_text(task_id, task_value)} for task_value in
                                        task_values]
    options['initial_task_ids'] = {task_id: task_values[0]['value'] for task_id, task_values in
                                   options['task_ids'].items()}

    # set `intervals` and `initial_interval`
    options['intervals'] = ["0%", "50%", "95%"]
    options['initial_interval'] = options['intervals'][-1]

    # set `available_as_ofs`, `initial_as_of`, and `current_date`
    # available_as_ofs is the subset of hub_config.reference_dates for which
    # there is at least one model output file
    options['available_as_ofs'] = hub_config.get_available_as_ofs()
    options['initial_as_of'] = max([get_max_ref_date_or_first_config_ref_date(reference_dates)
                                    for reference_dates in options['available_as_ofs'].values()])
    options['current_date'] = options['initial_as_of']

    # set `models` and `initial_checked_models`
    options['models'] = []
    for model_id, metadata in hub_config.model_id_to_metadata.items():
        if metadata['designated_model'] or model_id in hub_config.initial_checked_models:
            options['models'].append(model_id)
    options['models'].sort()
    options['initial_checked_models'] = hub_config.initial_checked_models

    # add `disclaimer` if present
    if hub_config.disclaimer is not None:
        options['disclaimer'] = hub_config.disclaimer

    # set `initial_xaxis_range`
    options['initial_xaxis_range'] = None

    return options
