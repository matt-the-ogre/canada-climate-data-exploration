import argparse
import logging
import time

from print_current_stations_bc_with_daily import get_stations_with_current_daily_data
from import_daily_by_station import import_csv_files
from combine_daily_data_by_station import concatenate_csv_files

from get_daily_data_by_station import download_all_station_data

# This script assumes all the CSV files are current and downloaded
# It will import all the CSV files for all the stations in BC into the database

def main(args):
    debug = args.debug
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # get the list of stations in BC with current data (up to the current year)
    st = time.process_time()

    full_stations_list = get_stations_with_current_daily_data()

    et = time.process_time()
    logging.info(f"Time to get stations with current data: {et - st} seconds")
    logging.debug(f"Full stations list: {full_stations_list}")

    total_stations = len(full_stations_list)
    
    # download the CSV files for each station
    for index, station in enumerate(full_stations_list):
        logging.info(f"Processing station {station} ({index+1}/{total_stations})")
        logging.debug(f"Station: {station}")
        st = time.process_time()
        download_all_station_data(station, debug=True)
        et = time.process_time()
        logging.info(f"Time to download CSV files for station {station}: {et - st} seconds")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Download all daily data for all stations in BC to the database")
    parser.add_argument('--debug', type=bool, default=False, help="Debug flag")
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    main(args)