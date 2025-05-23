import json
import re
from datetime import date
from pathlib import Path

import click
import pandas as pd
import structlog

from hub_predtimechart.generate_data import forecast_data_for_model_df
from hub_predtimechart.generate_options import ptc_options_for_hub
from hub_predtimechart.hub_config_ptc import HubConfigPtc
from hub_predtimechart.util.logs import setup_logging


setup_logging()
logger = structlog.get_logger()


@click.command()
@click.argument('hub_dir', type=click.Path(file_okay=False, exists=True))
@click.argument('ptc_config_file', type=click.Path(file_okay=True, exists=False))
@click.argument('options_file_out', type=click.Path(file_okay=True, exists=False))
@click.argument('forecasts_out_dir', type=click.Path(file_okay=False, exists=True))
@click.option('--regenerate', is_flag=True, default=False)
def main(hub_dir, ptc_config_file, options_file_out, forecasts_out_dir, regenerate):
    """
    Generates the options json file and forecast json files used by https://github.com/reichlab/predtimechart to
    visualize a hub's forecasts.

    HUB_DIR: (input) a directory Path of a https://docs.hubverse.io hub to generate forecast json files from

    PTC_CONFIG_FILE: (input) a file Path to a `predtimechart-config.yaml` file that specifies how to process `hub_dir`
    to get predtimechart output

    OPTIONS_FILE_OUT: (output) a file Path to output the predtimechart options object file to (see
    https://github.com/reichlab/predtimechart?tab=readme-ov-file#options-object )

    FORECASTS_OUT_DIR: (output) a directory Path to output the viz forecast json files to

    --REGENERATE: (flag) indicator for a complete rebuild of the data regardless of whether the files exist.
    \f
    :param hub_dir: (input) a directory Path of a https://docs.hubverse.io hub to generate forecast json files from
    :param ptc_config_file: (input) a file Path to a `predtimechart-config.yaml` file that specifies how to process
        `hub_dir` to get predtimechart output
    :param options_file_out: (output) a file Path to output the predtimechart options object file to (see
        https://github.com/reichlab/predtimechart?tab=readme-ov-file#options-object )
    :param forecasts_out_dir: (output) a directory Path to output the viz forecast json files to
    :param regenerate: (flag) indicator for a complete rebuild of the data regardless of whether the files exist.
    """
    logger.info(f"main({hub_dir=}, {ptc_config_file=}, {options_file_out=}, {forecasts_out_dir=}, {regenerate=}): "
                f"entered")
    hub_config = HubConfigPtc(Path(hub_dir), Path(ptc_config_file))
    json_files = _generate_forecast_json_files(hub_config, Path(forecasts_out_dir), regenerate)
    _generate_options_file(hub_config, Path(options_file_out))
    logger.info(f"main(): done: {len(json_files)} JSON files generated: {[str(_) for _ in json_files]}. "
                f"config file generated: {options_file_out}")


#
# _generate_forecast_json_files() and helpers
#

def _generate_forecast_json_files(hub_config: HubConfigPtc, output_dir: Path, is_regenerate: bool = False) \
        -> list[Path]:
    """
    Generates forecast json files from `hub_config`. Returns a list of Paths of the generated files.

    :param hub_config: see caller above
    :param output_dir: ""
    :param is_regenerate: boolean indicator for a complete rebuild of the data regardless of whether the files exist.
    """
    # for each ModelTask in hub_config, loop over every (reference_date X model_id) combination. the nested order of
    # reference_date, model_id ensures we open each model_output file only once. the tradeoff is that all model_output
    # files for a particular reference_date are loaded into memory, but that should be reasonable given the number of
    # teams a hub might have and the size of their model_output files
    json_files = []  # list of files actually generated
    for model_task in hub_config.model_tasks:
        available_ref_dates = model_task.get_available_ref_dates()
        newest_reference_date = max([date.fromisoformat(date_str) for date_str in available_ref_dates]).isoformat()
        df_cols_to_use = ([model_task.viz_target_col_name] + model_task.viz_task_ids +
                          [hub_config.target_date_col_name, 'output_type', 'output_type_id', 'value'])
        for reference_date in model_task.viz_reference_dates:  # ex: ['2022-10-22', '2022-10-29', ...]
            # set model_id_to_df
            model_id_to_df: dict[str, pd.DataFrame] = {}
            for model_id in hub_config.model_id_to_metadata:  # ex: ['Flusight-baseline', 'MOBS-GLEAM_FLUH', ...]
                model_output_file = hub_config.model_output_file_for_ref_date(model_id, reference_date)
                if model_output_file:
                    if model_output_file.suffix == '.csv':
                        model_id_to_df[model_id] = pd.read_csv(model_output_file, usecols=df_cols_to_use)
                    elif model_output_file.suffix in ['.parquet', '.pqt']:
                        model_id_to_df[model_id] = pd.read_parquet(model_output_file, columns=df_cols_to_use)
                    else:
                        raise RuntimeError(f"unsupported model output file type: {model_output_file!r}. "
                                           f"Only .csv and .parquet are supported")

            if not model_id_to_df:  # no model outputs for reference_date
                continue

            # iterate over each (target X task_ids) combination (for now we only support one target), outputting to the
            # corresponding json file
            for task_ids_tuple in model_task.viz_task_ids_tuples:
                json_file = generate_forecast_json_file(hub_config, model_id_to_df, output_dir,
                                                        model_task.viz_target_id, task_ids_tuple, reference_date,
                                                        newest_reference_date, is_regenerate)
                if json_file:
                    json_files.append(json_file)

    # done
    return json_files


def generate_forecast_json_file(hub_config, model_id_to_df, output_dir, target, task_ids_tuple, reference_date,
                                newest_reference_date, is_regenerate):
    """
    Gets the forecast data to save using the passed args and then saves it to the appropriately-named json file in
    `output_dir`. Returns the saved json file Path, or None if no json file was generated (i.e., there was no forecast
    data for the args) OR if the json file already exists and is not the current round.
    """
    file_name = json_file_name(target, task_ids_tuple, reference_date)
    json_file_path = output_dir / file_name
    if not is_regenerate and (reference_date != newest_reference_date) and Path(json_file_path).exists():
        return None

    forecast_data = {}
    for model_id, model_df in model_id_to_df.items():
        model_forecast_data = forecast_data_for_model_df(hub_config, model_df, target, task_ids_tuple)
        if model_forecast_data:
            forecast_data[model_id] = model_forecast_data

    if forecast_data:
        with open(json_file_path, 'w') as fp:
            json.dump(forecast_data, fp, indent=4, default=str)
            return json_file_path

    return None


def json_file_name(target: str, task_ids_tuple: tuple[str], reference_date: str) -> str:
    """
    Top level function that returns a file name that encodes the passed arguments. Args are per the `_fetchData()`
    signature documented at https://github.com/reichlab/predtimechart?tab=readme-ov-file#appinitialize-args :
        `_fetchData(isForecast, targetKey, taskIDs, referenceDate)`

    The translation is one way, that is, you can only encode the args into a file name, but you cannot go the other
    direction and retrieve the args from the file name.

    :param target: string naming the target of interest
    :param task_ids_tuple: a tuple of task id values. ex: ('US', 'A-2021-03-05'). NB: assumes these are sorted according
        to HubConfigPtc.viz_task_ids
    :param reference_date: string naming the reference_date of interest
    :return: a "valid" file name
    """


    def replace_chars(the_string: str) -> str:
        # replace all non-alphanumeric characters, except dashes and underscores, with a dash
        return re.sub(r'[^a-zA-Z0-9-_]', '-', the_string)


    target_str = replace_chars(target)
    task_ids_str = replace_chars('_'.join(task_ids_tuple))
    return f"{target_str}_{task_ids_str}_{reference_date}.json"


#
# _generate_options_file()
#

def _generate_options_file(hub_config: HubConfigPtc, options_file: Path):
    """
    Generates a predtimechart config .json file from `hub_config` as documented at `ptc_options_for_hub()`, saving it to
    `options_file`. NB: `options_file` is overwritten if already present.
    """
    options = ptc_options_for_hub(hub_config)
    with open(options_file, 'w') as fp:
        json.dump(options, fp, indent=4)


#
# main()
#

if __name__ == "__main__":
    main()
