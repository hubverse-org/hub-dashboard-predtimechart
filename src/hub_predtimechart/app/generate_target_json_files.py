import json
import sys
from datetime import date
from pathlib import Path

import click
import pandas as pd
import polars as pl
import structlog

from hub_predtimechart.app.generate_json_files import json_file_name
from hub_predtimechart.hub_config_ptc import HubConfigPtc, ModelTask
from hub_predtimechart.util.logs import setup_logging


setup_logging()
logger = structlog.get_logger()


@click.command()
@click.argument('hub_dir', type=click.Path(file_okay=False, exists=True))
@click.argument('ptc_config_file', type=click.Path(file_okay=True, exists=False))
@click.argument('target_out_dir', type=click.Path(file_okay=False, exists=True))
@click.option('--regenerate', is_flag=True, default=False)
def main(hub_dir, ptc_config_file, target_out_dir, regenerate):
    """
    Generates the target data json files used by https://github.com/reichlab/predtimechart to visualize a hub's
    forecasts. Handles missing input target data in two ways, depending on the error. 1) If the `target_data_file_name`
    entry in the hub config file is missing, then the program will exit with no messages. 2) If the entry is present but
    the file it points to does not exist, then the program will exit with an error message, but won't actually raise a
    Python exception.

    HUB_DIR: (input) a directory Path of a https://hubverse.io hub to generate target data json files from

    PTC_CONFIG_FILE: (input) a file Path to a `predtimechart-config.yaml` file that specifies how to process `hub_dir`
    to get predtimechart output

    TARGET_OUT_DIR: (output) a directory Path to output the viz target data json files to

    --REGENERATE: (flag) indicator for a complete rebuild of the data regardless of whether the files exist.
    \f
    :param hub_dir: (input) a directory Path of a https://hubverse.io hub to generate target data json files from
    :param ptc_config_file: (input) a file Path to a `predtimechart-config.yaml` file that specifies how to process
        `hub_dir` to get predtimechart output
    :param target_out_dir: (output) a directory Path to output the viz target data json files to
    :param regenerate: (flag) indicator for a complete rebuild of the data regardless of whether the files exist.
    """
    logger.info(f'main({hub_dir=}, {target_out_dir=}, {regenerate=}): entered')
    hub_config = HubConfigPtc(Path(hub_dir), Path(ptc_config_file))

    try:
        target_data_df = hub_config.get_target_data_df()
    except FileNotFoundError as error:
        logger.error(f"target data file not found. {hub_config.get_target_data_file_name()=}, {error=}")
        sys.exit(1)

    json_files = _generate_target_json_files(hub_config, target_data_df, target_out_dir, regenerate)
    logger.info(f'main(): done: {len(json_files)} JSON files generated: {[str(_) for _ in json_files]}. ')


def _generate_target_json_files(hub_config: HubConfigPtc, target_data_df: pd.DataFrame, target_out_dir: Path,
                                is_regenerate: bool = False) -> list[Path]:
    """
    Generates target json files from `hub_config`. Returns a list of Paths of the generated files.

    :param hub_config: see caller above
    :param target_data_df: ""
    :param target_out_dir: ""
    :param is_regenerate: boolean indicator for a complete rebuild of the data regardless of whether the files exist.
    """
    json_files = []  # list of files actually generated
    # for each (model_task x reference_date x task_ids_tuple) combination, generate and save target data as a json file
    target_out_dir = Path(target_out_dir)
    for model_task in hub_config.model_tasks:
        for reference_date in model_task.viz_reference_dates:
            for task_ids_tuple in model_task.viz_task_ids_tuples:
                file_name = json_file_name(model_task.viz_target_id, task_ids_tuple, reference_date)
                file_p = target_out_dir / file_name
                if not is_regenerate and file_p.exists():
                    continue  # skip existing file

                location_data_dict = ptc_target_data(model_task, target_data_df, task_ids_tuple, reference_date)
                if not location_data_dict:
                    continue  # no data

                json_files.append(file_p)
                with open(file_p, 'w') as fp:
                    json.dump(location_data_dict, fp, indent=4)
    return json_files


def ptc_target_data(model_task: ModelTask, target_data_df: pl.DataFrame, task_ids_tuple: tuple[str],
                    reference_date: str | None) -> dict[str, list] | None:
    """
    Returns a dict for a single reference date and location in the target data format documented at https://github.com/reichlab/predtimechart?tab=readme-ov-file#fetchdata-truth-data-format.
    Note that this function currently assumes there is only one task id variable other than the reference date, horizon,
    and target date, and that task id variable is a location code that matches codes used in the `location` column of
    the `target_data_df` argument. That is, looking at that example, this function returns the date and value columns as
    in tests/expected/FluSight-forecast-hub/target/wk-inc-flu-hosp_US.json :

    {
      "date": ["2024-04-27", "2024-04-20", "..."],
      "y": [2337, 2860, "..."]
    }

    :param model_task: a ModelTask from HubConfigPtc
    :param target_data_df: a pl.DataFrame that loaded from HubConfigPtc.target_data_file_name
    :param task_ids_tuple: a tuple as returned by HubConfigPtc.fetch_task_ids_tuples
    :param reference_date: a date from `hub_config.viz_reference_dates` that's used to find the closest as_of in
        `target_data_df` if that column is present. ignored if column is not present
    :return a dict as documented above with two keys: 'date' and 'y' if data was found. o/w return None
    """
    # filter to max as_of that's <= reference_date if no hub_config.target_data_file_name and as_of column present in
    # target_data_df
    if (not model_task.hub_config_ptc.target_data_file_name) and ('as_of' in target_data_df.columns):
        max_as_of = _max_as_of_le_reference_date(target_data_df, reference_date)
        if max_as_of is None:
            return None
        else:
            target_data_df = target_data_df.filter(pl.col('as_of') == max_as_of.isoformat())

    # until all hubs implement our new time-series target data standard, we condition on
    # hub_config.target_data_file_name, which acts as a flag indicating whether the hub implements the new standard or
    # not
    if model_task.hub_config_ptc.target_data_file_name:
        target_date_col_name = 'date'
        observation_col_name = 'value'
    else:
        target_date_col_name = model_task.hub_config_ptc.target_date_col_name
        observation_col_name = 'observation'

    if not model_task.hub_config_ptc.target_data_file_name:
        target_data_df = target_data_df.filter(pl.col(model_task.viz_target_col_name) == model_task.viz_target_id)
    for task_id, task_id_value in zip(model_task.viz_task_id_to_vals, task_ids_tuple):
        target_data_df = target_data_df.filter(pl.col(task_id) == task_id_value)
    target_data_df = target_data_df.sort(target_date_col_name)

    if len(target_data_df) == 0:
        return None

    return {
        'date': target_data_df[target_date_col_name].to_list(),
        'y': target_data_df[observation_col_name].to_list()
    }


def _max_as_of_le_reference_date(target_data_df: pl.DataFrame, reference_date: str) -> date:
    """
    ptc_target_data() helper

    :return: max as_of that's <= reference_date if hub_config.target_data_file_name and as_of column present in
        target_data_df. return None if not found
    """
    unique_as_ofs = [date.fromisoformat(as_of) for as_of in
                     pl.Series(target_data_df.unique('as_of').select('as_of').sort('as_of'))]  # sort for debugging
    le_as_ofs = [as_of for as_of in unique_as_ofs if as_of <= date.fromisoformat(reference_date)]
    return max(le_as_ofs) if le_as_ofs else None


#
# main()
#

if __name__ == '__main__':
    main()
