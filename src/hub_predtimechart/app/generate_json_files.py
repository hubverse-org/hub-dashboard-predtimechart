import itertools
import json
import re
from pathlib import Path

import click
import pandas as pd
import structlog

from hub_predtimechart.generate_data import forecast_data_for_model_df
from hub_predtimechart.hub_config import HubConfig
from hub_predtimechart.util.logs import setup_logging


setup_logging()
logger = structlog.get_logger()


@click.command()
@click.argument('hub_dir', type=click.Path(file_okay=False, exists=True))
@click.argument('output_dir', type=click.Path(file_okay=False, exists=True))
def main(hub_dir, output_dir):
    """
    An app that generates predtimechart forecast json files from `hub_dir`, outputting to `output_dir`.

    :param hub_dir: a Path of a hub to generate forecast json files from
    :param output_dir: a Path to output forecast json files to
    """
    logger.info(f"main({hub_dir=}, {output_dir=}): entered")
    json_files = _main(HubConfig(Path(hub_dir)), Path(output_dir))
    logger.info(f"main(): done: {len(json_files)} files generated: {[str(_) for _ in json_files]}")


def _main(hub_config: HubConfig, output_dir: Path):
    # loop over every (reference_date X model_id) combination. the nested order of reference_date, model_id ensures we
    # open each model_output file only once. the tradeoff is that all model_output files for a particular reference_date
    # are loaded into memory, but that should be reasonable given the number of teams a hub might have and the size of
    # their model_output files
    df_cols_to_use = ([hub_config.target_col_name] + hub_config.viz_task_ids +
                      [hub_config.target_date_col_name, 'output_type', 'output_type_id', 'value'])
    json_files = []  # list of files actually loaded
    for reference_date in hub_config.fetch_reference_dates:  # ex: ['2022-10-22', '2022-10-29', ...]
        # set model_id_to_df
        model_id_to_df: dict[str, pd.DataFrame] = {}
        for model_id in hub_config.model_id_to_metadata:  # ex: ['Flusight-baseline', 'MOBS-GLEAM_FLUH', ...]
            model_output_file = hub_config.model_output_file_for_ref_date(model_id, reference_date)
            if model_output_file:
                model_id_to_df[model_id] = pd.read_csv(model_output_file, usecols=df_cols_to_use)

        if not model_id_to_df:  # no model outputs for reference_date
            continue

        # iterate over each (target X task_ids) combination, outputting to the corresponding json file
        for target, task_ids_tuple in itertools.product(hub_config.fetch_targets, hub_config.fetch_task_ids_tuples):
            json_file = generate_forecast_json_file(hub_config, model_id_to_df, output_dir, target, task_ids_tuple,
                                                    reference_date)
            if json_file:
                json_files.append(json_file)

    # done
    return json_files


def generate_forecast_json_file(hub_config, model_id_to_df, output_dir, target, task_ids_tuple, reference_date):
    """
    Gets the forecast data to save using the passed args and then saves it to the appropriately-named json file in
    `output_dir`. Returns the saved json file Path, or None if no json file was generated (i.e., there was no forecast
    data for the args).
    """
    forecast_data = {}
    for model_id, model_df in model_id_to_df.items():
        model_forecast_data = forecast_data_for_model_df(hub_config, model_df, target, task_ids_tuple)
        if model_forecast_data:
            forecast_data[model_id] = model_forecast_data

    if forecast_data:
        file_name = json_file_name(target, task_ids_tuple, reference_date)
        json_file_path = output_dir / file_name
        with open(json_file_path, 'w') as fp:
            json.dump(forecast_data, fp, indent=4)
            return json_file_path

    return None


#
# json_file_name()
#

def json_file_name(target: str, task_ids_tuple: tuple[str], reference_date: str) -> str:
    """
    Top level function that returns a file name that encodes the passed arguments. Args are per the `_fectchData()`
    signature documented at https://github.com/reichlab/predtimechart?tab=readme-ov-file#appinitialize-args :
        `_fetchData(isForecast, targetKey, taskIDs, referenceDate)`

    The translation is one way, that is, you can only encode the args into a file name, but you cannot go the other
    direction and retrieve the args from the file name.

    :param target: "". a tuple of task id values. ex: ('US', 'A-2021-03-05')
    :param task_ids_tuple: "". NB: assumes these are sorted!
    :param reference_date: ""
    :return: a "valid" file name
    """
    return _get_valid_filename(f"{target}_{'_'.join(task_ids_tuple)}_{reference_date}.json")


# based on get_valid_filename() from https://github.com/django/django/blob/main/django/utils/text.py
def _get_valid_filename(name):
    """
    Return the given string converted to a string that can be used for a clean
    filename. Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    >>> _get_valid_filename("john's portrait in 2004.jpg")
    'johns_portrait_in_2004.jpg'
    """
    s = str(name).strip().replace(" ", "-")  # was "_"
    s = re.sub(r"(?u)[^-\w.]", "", s)
    if s in {"", ".", ".."}:
        raise RuntimeError("Could not derive file name from '%s'" % name)  # was: SuspiciousFileOperation
    return s


#
# main()
#

if __name__ == "__main__":
    main()
