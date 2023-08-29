import pandas as pd
import logging
import argparse

# This program takes a csv file and cleans it and replaces the original csv file with the cleaned version

def clean_daily_data(csv_file):

    # read in the csv file
    df = pd.read_csv(csv_file)

    # backup the original csv file
    df.to_csv(f"{csv_file}.bak", index=False)

    # print out some information about the dataframe
    # logging.debug(f"df.columns: {df.columns}")
    # logging.debug(f"df.head(): {df.head()}")
    # logging.debug(f"df.tail(): {df.tail()}")
    # logging.debug(f"df.dtypes: {df.dtypes}")
    # logging.debug(f"df.shape: {df.shape}")
    # logging.debug(f"df.describe(): {df.describe()}")
    # logging.debug(f"df.info(): {df.info()}")
    # logging.debug(f"df.isnull().sum(): {df.isnull().sum()}")

    # drop the columns that are not needed
    # df = df.drop(columns=['Longitude (x)', 'Latitude (y)', 'Station Name', 'Climate ID', 'Data Quality', 'Max Temp Flag', 'Min Temp Flag', 'Mean Temp Flag', 'Heat Deg Days Flag', 'Cool Deg Days Flag', 'Total Rain Flag', 'Total Snow Flag', 'Total Precip Flag', 'Snow on Grnd Flag', 'Dir of Max Gust Flag', 'Spd of Max Gust Flag'])
    # df = df.drop(columns=['Longitude (x)', 'Latitude (y)', 'Station Name', 'Climate ID'], inplace=True, axis=1)
    if 'Station Name' in df.columns:
        df.drop(columns=['Longitude (x)', 'Latitude (y)', 'Station Name', 'Climate ID'], inplace=True, axis=1)
        # logging.debug(f"df.columns: {df.columns}")
    
    # drop the columns that are not needed
    if 'Max Temp Flag' in df.columns:
        df.drop(columns=['Max Temp Flag', 'Min Temp Flag', 'Mean Temp Flag', 'Heat Deg Days Flag', 'Cool Deg Days Flag', 'Total Rain Flag', 'Total Snow Flag', 'Total Precip Flag', 'Snow on Grnd Flag', 'Dir of Max Gust Flag', 'Spd of Max Gust Flag'], inplace=True, axis=1)
        # logging.debug(f"df.columns: {df.columns}")

    # drop the rows where 'Data Quality' is null
    # df = df.dropna(subset=['Data Quality'])
    # logging.debug(f"df.shape: {df.shape}")
    df.dropna(subset=['Data Quality'], inplace=True)

    # convert the special characters in the 'Data Quality' column to True
    df['Data Quality'] = df['Data Quality'].apply(lambda x: True if x == '†' else x)

    # logging.debug(f"df.isnull().sum(): {df.isnull().sum()}")
    # logging.debug(f"df.shape: {df.shape}")

    # check for duplicate rows
    num_duplicated_rows = df.duplicated().sum()
    if num_duplicated_rows:
        logging.debug(f"df.duplicated().sum(): {num_duplicated_rows}")
        # drop the duplicate rows
        df.drop_duplicates(inplace=True)
    
    # write the cleaned dataframe to a csv file
    df.to_csv(csv_file, index=False)
    logging.info(f"{csv_file} written to disk")

def parse_arguments():
    parser = argparse.ArgumentParser(description="clean daily data")
    parser.add_argument('csv_file', type=str, help="daily data csv file to clean")
    parser.add_argument('--debug', type=bool, default=False, help="debug mode")
    # parser.add_argument('--database_path', type=str, default='../data/database.db', help="Path to the SQLite database")
    return parser.parse_args()

def main(args):
    debug = args.debug
    # logging.info(f"debug: {debug}")
    csv_file = args.csv_file
    # logging.info(f"csv_file: {csv_file}")
    clean_daily_data(csv_file)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    args = parse_arguments()
    main(args)