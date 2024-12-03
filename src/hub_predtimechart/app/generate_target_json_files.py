import json
from datetime import date, timedelta
from pathlib import Path

import click
import polars as pl
import structlog

from hub_predtimechart.app.generate_json_files import json_file_name
from hub_predtimechart.hub_config import HubConfig
from hub_predtimechart.util.logs import setup_logging


setup_logging()
logger = structlog.get_logger()


@click.command()
@click.argument('hub_dir', type=click.Path(file_okay=False, exists=True))
@click.argument('ptc_config_file', type=click.Path(file_okay=True, exists=False))
@click.argument('target_out_dir', type=click.Path(file_okay=False, exists=True))
def main(hub_dir, ptc_config_file, target_out_dir):
    """
    Generates the target data json files used by https://github.com/reichlab/predtimechart to
    visualize a hub's forecasts. Handles missing input target data in two ways, depending on the error. 1) If the
    `target_data_file_name` entry in the hub config file is missing, then the program will exit with no messages.
    2) If the entry is present but the file it points to does not exist, then the program will exit with an error
    message, but won't actually raise a Python exception.

    HUB_DIR: (input) a directory Path of a https://hubverse.io hub to generate target data json files from

    PTC_CONFIG_FILE: (input) a file Path to a `predtimechart-config.yaml` file that specifies how to process `hub_dir`
    to get predtimechart output

    TARGET_OUT_DIR: (output) a directory Path to output the viz target data json files to
    \f
    :param hub_dir: (input) a directory Path of a https://hubverse.io hub to generate target data json files from
    :param ptc_config_file: (input) a file Path to a `predtimechart-config.yaml` file that specifies how to process
        `hub_dir` to get predtimechart output
    :param target_out_dir: (output) a directory Path to output the viz target data json files to
    """
    logger.info(f'main({hub_dir=}, {target_out_dir=}): entered')

    hub_dir = Path(hub_dir)
    hub_config = HubConfig(hub_dir, Path(ptc_config_file))
    if hub_config.target_data_file_name is None:
        logger.info('No `target_data_file_name` entry found in hub config file. exiting')
        return

    # for each location, generate target data file contents and then save as json
    json_files = []
    try:
        target_data_df = get_target_data_df(hub_dir, hub_config.target_data_file_name)
    except FileNotFoundError as error:
        logger.error(f"target data file not found. {hub_config.target_data_file_name=}, {error=}")
        return

    for loc in target_data_df['location'].unique():
        task_ids_tuple = (loc,)
        target_out_dir = Path(target_out_dir)
        file_name = json_file_name(hub_config.fetch_target_id, task_ids_tuple, reference_date_from_today().isoformat())
        location_data_dict = ptc_target_data(target_data_df, task_ids_tuple)
        json_files.append(target_out_dir / file_name)
        with open(target_out_dir / file_name, 'w') as fp:
            json.dump(location_data_dict, fp, indent=4)

    logger.info(f'main(): done: {len(json_files)} JSON files generated: {[str(_) for _ in json_files]}. ')


def get_target_data_df(hub_dir, target_data_filename):
    """
    Loads the target data csv file from the hub repo for now, file path for target data is hard coded to 'target-data'.
    Raises FileNotFoundError if target data file does not exist.
    """
    if target_data_filename is None:
        raise FileNotFoundError(f"target_data_filename was missing: {target_data_filename}")

    target_data_file_path = hub_dir / 'target-data' / target_data_filename
    try:
        # the override schema handles the 'US' location (the only location that doesn't parse as Int64)
        return pl.read_csv(target_data_file_path, schema_overrides={'location': pl.String}, null_values=["NA"])
    except FileNotFoundError as error:
        raise FileNotFoundError(f"target data file not found. {target_data_file_path=}, {error=}")


def ptc_target_data(target_data_df: pl.DataFrame, task_ids_tuple: tuple[str]):
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
    """
    loc = task_ids_tuple[0]
    target_data_loc = target_data_df.filter(pl.col('location') == loc).sort('date')
    target_data_ptc = {
        "date": target_data_loc['date'].to_list(),
        "y": target_data_loc['value'].to_list()
    }

    return target_data_ptc


def reference_date_from_today(now: date = None) -> date:
    if now is None:  # per https://stackoverflow.com/questions/52511405/freeze-time-not-working-for-default-param
        now = date.today()

    # Calculate the days until the next Saturday
    days_to_saturday = 5 - now.weekday()
    if days_to_saturday < 0:
        days_to_saturday += 7

    # Add the calculated days to the given date
    return now + timedelta(days=days_to_saturday)


#
# main()
#

if __name__ == '__main__':
    main()
