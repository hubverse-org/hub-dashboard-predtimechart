from datetime import date, datetime, timedelta
import json
import re
from pathlib import Path

import click
import polars as pl
import structlog

from hub_predtimechart.generate_target_data import target_data_for_FluSight
from hub_predtimechart.app.generate_json_files import json_file_name
from hub_predtimechart.util.logs import setup_logging


setup_logging()
logger = structlog.get_logger()


@click.command()
@click.argument('hub_dir', type=click.Path(file_okay=False, exists=True))
@click.argument('target_out_dir', type=click.Path(file_okay=False, exists=True))
def main(hub_dir, target_out_dir):
    '''
    Generates the target data json files used by https://github.com/reichlab/predtimechart to
    visualize a hub's forecasts.

    HUB_DIR: (input) a directory Path of a https://hubverse.io hub to generate target data json files from

    TARGET_OUT_DIR: (output) a directory Path to output the viz target data json files to
    \f
    :param hub_dir: (input) a directory Path of a https://hubverse.io hub to generate target data json files from
    :param target_out_dir: (output) a directory Path to output the viz target data json files to

    '''
    logger.info(f'main({hub_dir=}, {target_out_dir=}): entered')
    
    hub_dir = Path(hub_dir)
    target_out_dir = Path(target_out_dir)
    
    # load the target data csv file from the hub repo
    # for now, file path for target data is hard coded
    target_data_df = pl.read_csv(hub_dir / 'target-data/target-hospital-admissions.csv')
    
    # for each location,
    # - generate target data file contents
    # - save as json
    json_files = []
    for loc in target_data_df['location'].unique():
        task_ids_tuple = (loc,)
        location_data_dict = target_data_for_FluSight(target_data_df, task_ids_tuple)
        file_name = json_file_name('wk inc flu hosp', task_ids_tuple, reference_date_from_today())
        json_files.append(target_out_dir / file_name)
        with open(target_out_dir / file_name, 'w') as fp:
            json.dump(location_data_dict, fp, indent=4)

    logger.info(f'main(): done: {len(json_files)} JSON files generated: {[str(_) for _ in json_files]}. ')
    


#
# _generate_json_files() and helpers
#
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
