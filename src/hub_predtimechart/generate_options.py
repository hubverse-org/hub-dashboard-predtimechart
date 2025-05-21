from collections import defaultdict

from hub_predtimechart.hub_config_ptc import HubConfigPtc


def ptc_options_for_hub(hub_config: HubConfigPtc):
    """
    Returns predtimechart options dict for `hub_config` - see https://github.com/reichlab/predtimechart?tab=readme-ov-file#options-object .

    :param hub_config: a HubConfigPtc
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
            return min(hub_config.viz_reference_dates)
        else:
            return max(reference_dates)


    # set `target_variables` and `initial_target_var`. recall: we require exactly one `target_metadata` entry, and only
    # one entry under its `target_keys`
    options = {}
    options['target_variables'] = []
    for model_task in hub_config.model_tasks:
        target_metadata = model_task.task['target_metadata'][0]
        target_name = target_metadata['target_name']  # ex: "Percentage of ED visits due to influenza"
        target_key_value = list(target_metadata['target_keys'].values())[0]  # ex: "Flu ED visits pct"
        options['target_variables'].append({'value': target_key_value,
                                            'text': target_name,
                                            'plot_text': target_name})
    options['initial_target_var'] = options['target_variables'][0]['value']  # arbitrary choice

    # set `task_ids` and `initial_task_ids`
    options['task_ids'] = {}
    for model_task in hub_config.model_tasks:
        options['task_ids'][model_task.viz_target_id] = defaultdict(list)
        for task_id, task_values in model_task.viz_task_id_to_vals.items():  # ex: {'location': ["US", "01", ...], ...}
            options['task_ids'][model_task.viz_target_id][task_id].extend(
                [{'value': task_value, 'text': task_text(task_id, task_value)} for task_value in task_values])
        options['task_ids'][model_task.viz_target_id] = dict(options['task_ids'][model_task.viz_target_id])  # convert
    options['initial_task_ids'] = {task_id: task_values[0]['value']  # arbitrary choice
                                   for task_id, task_values
                                   in options['task_ids'][options['initial_target_var']].items()}

    # set `intervals` and `initial_interval`
    options['intervals'] = ["0%", "50%", "95%"]
    options['initial_interval'] = options['intervals'][-1]

    # set `available_as_ofs`, `initial_as_of`, and `current_date`
    options['available_as_ofs'] = {}
    for model_task in hub_config.model_tasks:
        options['available_as_ofs'][model_task.viz_target_id] = model_task.get_available_ref_dates()
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
    options['initial_xaxis_range'] = hub_config.initial_xaxis_range if hub_config.initial_xaxis_range else None

    return options
