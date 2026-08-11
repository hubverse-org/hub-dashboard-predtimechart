import json
from pathlib import Path

import pytest

from hub_predtimechart.generate_options import ptc_options_for_hub, _host_owner_name
from hub_predtimechart.hub_config_ptc import HubConfigPtc, ModelTask


def test_generate_options_complex_forecast_hub():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with open('tests/expected/example-complex-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    for exp_field in ['target_variables', 'initial_target_var', 'task_ids', 'initial_task_ids', 'intervals',
                      'initial_interval', 'available_as_ofs', 'initial_as_of', 'current_date', 'models',
                      'initial_checked_models', 'disclaimer', 'initial_xaxis_range']:
        assert exp_field in act_options
        assert act_options[exp_field] == exp_options[exp_field]

    assert act_options == exp_options


def test_generate_options_complex_forecast_hub_no_disclaimer():
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, Path('tests/configs/example-complex-no-disclaimer.yml'))
    with open('tests/expected/example-complex-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert 'disclaimer' not in act_options


def test_generate_options_flusight_forecast_hub():
    hub_dir = Path('tests/hubs/FluSight-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with open('tests/expected/FluSight-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert act_options == exp_options


def test_generate_options_flu_metrocast():
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with open('tests/expected/flu-metrocast/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert act_options == exp_options


def test_generate_options_multi_target_per_block():
    # regression for #88: when a single model_tasks block carries multiple target_metadata entries,
    # `target_variables` must contain one distinct entry per target (not the same entry repeated). simulate this
    # by loading a single-target hub and appending a second target_metadata entry plus a matching ModelTask with
    # target_metadata_idx=1, mirroring what HubConfigPtc builds for multi-target-per-block hubs
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    existing_block = hub_config.model_tasks[0].task
    existing_block['target_metadata'].append({
        'target_keys': {'target': 'second_target'},
        'target_name': 'second target readable name',
        'is_step_ahead': True,
    })
    hub_config.model_tasks.append(ModelTask(hub_config, existing_block, target_metadata_idx=1))

    options = ptc_options_for_hub(hub_config)
    target_values = [tv['value'] for tv in options['target_variables']]
    target_texts = [tv['text'] for tv in options['target_variables']]
    assert target_values == ['wk inc flu hosp', 'second_target']
    assert target_texts == ['incident influenza hospitalizations', 'second target readable name']
    # downstream keys must line up with target_variables
    assert set(options['available_as_ofs'].keys()) == set(target_values)
    assert set(options['task_ids'].keys()) == set(target_values)


#
# `intervals` tests
#

@pytest.mark.parametrize("hub_dir", ['tests/hubs/example-complex-forecast-hub',
                                     'tests/hubs/flu-metrocast',
                                     'tests/hubs/FluSight-forecast-hub',
                                     'tests/hubs/covid19-forecast-hub'])
def test_generate_options_intervals_80_pct_included(hub_dir):
    # all test hubs require the 0.1 and 0.9 quantile levels, so all of them are offered the optional "80%" interval
    hub_dir = Path(hub_dir)
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    act_options = ptc_options_for_hub(hub_config)
    assert act_options['intervals'] == ["0%", "50%", "80%", "95%"]
    assert act_options['initial_interval'] == "95%"


def test_generate_options_intervals_no_80_pct_backing_quantiles():
    # case: a hub with no forecast quantiles backing the 80% interval (no 0.1 or 0.9). it should be offered only the
    # required intervals - o/w selecting "80%" would plot nothing
    hub_dir = Path('tests/hubs/example-complex-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    output_type_id = hub_config.model_tasks[0].task['output_type']['quantile']['output_type_id']
    output_type_id['required'] = [0.025, 0.25, 0.5, 0.75, 0.975]  # no 0.1 or 0.9
    act_options = ptc_options_for_hub(hub_config)
    assert act_options['intervals'] == ["0%", "50%", "95%"]
    assert act_options['initial_interval'] == "95%"


@pytest.mark.parametrize("dropped_quantile_level", [0.1, 0.9])
def test_generate_options_intervals_partial_80_pct_backing_quantiles(dropped_quantile_level):
    # case: only one of the two quantile levels backing the 80% interval is present. an interval needs both of its
    # bounds, so "80%" must not be offered
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    for model_task in hub_config.model_tasks:
        output_type_id = model_task.task['output_type']['quantile']['output_type_id']
        output_type_id['required'] = [level for level in output_type_id['required']
                                      if level != dropped_quantile_level]
    assert ptc_options_for_hub(hub_config)['intervals'] == ["0%", "50%", "95%"]


def test_generate_options_intervals_80_pct_not_all_targets():
    # case: a multi-target hub where only *one* target has the backing quantiles. `intervals` is a single hub-wide
    # list, so "80%" must not be offered - it would plot nothing for the other target
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    assert len(hub_config.model_tasks) == 2  # the two targets are in separate model_tasks blocks
    output_type_id = hub_config.model_tasks[1].task['output_type']['quantile']['output_type_id']
    output_type_id['required'] = [0.025, 0.25, 0.5, 0.75, 0.975]  # no 0.1 or 0.9 for the second target only
    assert ptc_options_for_hub(hub_config)['intervals'] == ["0%", "50%", "95%"]

    # sanity check: with the first target similarly stripped and the second left alone, the result is the same
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    output_type_id = hub_config.model_tasks[0].task['output_type']['quantile']['output_type_id']
    output_type_id['required'] = [0.025, 0.25, 0.5, 0.75, 0.975]
    assert ptc_options_for_hub(hub_config)['intervals'] == ["0%", "50%", "95%"]


def test_generate_options_task_id_text_covid19_forecast_hub():
    hub_dir = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    with open('tests/expected/covid19-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    act_options = ptc_options_for_hub(hub_config)
    assert act_options == exp_options


def test_generate_options_initial_xaxis_range_covid19_forecast_hub():
    hub_dir = Path('tests/hubs/covid19-forecast-hub')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config-xaxis.yml')
    with open('tests/expected/covid19-forecast-hub/predtimechart-options.json') as fp:
        exp_options = json.load(fp)
    exp_options['initial_xaxis_range'] = hub_config.initial_xaxis_range
    act_options = ptc_options_for_hub(hub_config)
    assert act_options == exp_options


#
# model_urls tests
#

@pytest.mark.parametrize("hub_dir,exp_host_owner_name", [
    ('tests/hubs/covid19-forecast-hub',
     ('github', 'CDCgov', 'covid19-forecast-hub')),
    ('tests/hubs/example-complex-forecast-hub',
     ('github', 'Infectious-Disease-Modeling-Hubs', 'example-complex-forecast-hub')),
    ('tests/hubs/flu-metrocast',
     ('github', 'reichlab', 'flu-metrocast')),
    ('tests/hubs/FluSight-forecast-hub',
     ('github', 'cdcepi', 'FluSight-forecast-hub'))])
def test__host_owner_name(hub_dir, exp_host_owner_name):
    hub_dir = Path(hub_dir)
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    assert _host_owner_name(hub_config) == exp_host_owner_name


def test__host_owner_name_invalid_keys():
    hub_dir = Path('tests/hubs/flu-metrocast')
    hub_config = HubConfigPtc(hub_dir, hub_dir / 'hub-config/predtimechart-config.yml')
    hub_config.admin['repository'] = {'bad-repo': 0}
    with pytest.raises(RuntimeError, match="invalid admin repository keys"):
        _host_owner_name(hub_config)
