import pandas as pd

def main(args):
    # need to handle case where years are not available (0,0)
    # How do you do this with variable return values?
    name, province, first_year, last_year = get_daily_data_years(args.database_path, args.station_id)
    logging.info(f"daily data available for station {args.station_id} ({name}, {province}): {first_year} to {last_year}")
    
    # Example usage
    station_id = args.station_id
    timeframe = 2 # 1 = hourly, 2 = daily, 3 = monthly
    month = 1 # doesn't matter for daily data

    for year in range(first_year, last_year + 1):
        download_station_data(station_id, year, month, timeframe)
        time.sleep(1) # sleep for 1 second to avoid overloading the serve

def parse_arguments():
    parser = argparse.ArgumentParser(description="clean station data")
    parser.add_argument('station_id', type=int, help="Station ID to look up")
    parser.add_argument('--database_path', type=str, default='../data/database.db', help="Path to the SQLite database")
    return parser.parse_args()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args = parse_arguments()
    main(args)