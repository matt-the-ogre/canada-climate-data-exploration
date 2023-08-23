import schedule
import time
import logging
import os
from update_daily_data_bc_stations import update_database_daily

def daily_update():
    logging.info("Daily update run.")
    logging.debug(f"update start time: {time.time()}")
    # Check if the database file is present
    if not os.path.exists('data/database.db'):
        logging.error("Error: 'database.db' file not found. Aborting update.")
        return
    update_database_daily()
    logging.info("Daily update complete.")
    logging.debug(f"update end time: {time.time()}")
    last_update_time_filepath = "last_update_time.txt"
    with open(last_update_time_filepath, 'w') as file:
        file.write(str(time.time()))

def print_jobs():
    for job in schedule.jobs:
        # print(f"Job: {job.__name__}, Next run: {job.next_run}")
        logging.info(f"Job: {job.job_func} , Next run: {job.next_run}")

def main():
    # Schedule the daily update to run at a specific time (e.g., 2:00 AM)
    schedule.every().day.at("20:59").do(daily_update)
    schedule.every().day.at("21:00").do(daily_update)
    schedule.every().day.at("20:58").do(daily_update)
    schedule.every().day.at("04:01").do(daily_update)
    schedule.every().day.at("04:00").do(daily_update)
    schedule.every().day.at("04:02").do(daily_update)
    schedule.every().day.at("04:03").do(daily_update)
    schedule.every().day.at("04:04").do(daily_update)
    schedule.every().day.at("04:05").do(daily_update)
    schedule.every().day.at("04:06").do(daily_update)
    schedule.every().day.at("04:07").do(daily_update)
    schedule.every().day.at("04:08").do(daily_update)
    schedule.every().day.at("04:09").do(daily_update)
    schedule.every().day.at("04:10").do(daily_update)

    print_jobs()

    # Keep the script running to allow the scheduled job to execute
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.debug(f"main start time: {time.time()}")
    main()
