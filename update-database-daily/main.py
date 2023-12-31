import schedule
import time
from datetime import datetime
import logging
import os
from update_daily_data_bc_stations import update_database_daily
import argparse

def daily_update():
    logging.info("Daily update run.")
    logging.debug(f"update start time: {datetime.now()}")
    # Check if the database file is present
    if not os.path.exists('../data/database.db'):
        logging.error("Error: '../data/database.db' file not found. Aborting update.")
        return
    update_database_daily()
    logging.info("Daily update complete.")
    logging.debug(f"update end time: {datetime.now()}")
    date_right_now = datetime.now().strftime("%Y-%m-%d")
    last_update_time_filepath = "last_update_time.txt"
    with open(last_update_time_filepath, 'w') as file:
        file.write(str(datetime.now()))

def print_jobs():
    for job in schedule.jobs:
        logging.info(f"Job: {job.job_func} , Next run: {job.next_run}")

def main(args):
    # Schedule the daily update to run at a specific time (e.g., 2:00 AM)
    debug = args.debug

    schedule.every().day.at("09:00").do(daily_update)
    schedule.every().day.at("18:00").do(daily_update)
    if debug:
        schedule.every().minute.do(daily_update)


    if debug:
        print_jobs()

    if debug:
        daily_update()
    
    # Keep the script running to allow the scheduled job to execute
    while True:
        schedule.run_pending()
        time.sleep(10)

def parse_arguments():
    parser = argparse.ArgumentParser(description="Update the database with the latest data from BC stations")
    # parser.add_argument('station_id', type=int, help="Station ID to look up")
    parser.add_argument('--debug', type=bool, default=False, help="Run in debug mode")
    return parser.parse_args()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.debug(f"main start time: {datetime.now()}")
    args = parse_arguments()
    main(args)
