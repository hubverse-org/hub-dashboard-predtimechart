from hub_predtimechart.hub_config import HubConfig


def ptc_options_for_hub(hub_config: HubConfig):
    """
    Returns predtimechart options dict for `hub_config` - see https://github.com/reichlab/predtimechart?tab=readme-ov-file#options-object .

    :param hub_config: a HubConfig
    """
    options = {}

    # todo xx finish!

    # options['target_variables'] = None
    # options['initial_target_var'] = None
    # options['task_ids'] = None
    # options['initial_task_ids'] = None

    options['intervals'] = ["0%", "50%", "95%"]
    options['initial_interval'] = options['intervals'][-1]

    # options['available_as_ofs'] = hub_config.fetch_reference_dates
    # options['initial_as_of'] = None
    # options['current_date'] = None

    options['models'] = []
    for model_id, metadata in hub_config.model_id_to_metadata.items():
        if metadata['designated_model']:  # todo xx put here?: predtimechart-config.yml
            options['models'].append(model_id)
    options['models'].sort()  # todo xx order?

    options['initial_checked_models'] = hub_config.initial_checked_models
    options['disclaimer'] = hub_config.disclaimer
    options['initial_xaxis_range'] = None
    return options
