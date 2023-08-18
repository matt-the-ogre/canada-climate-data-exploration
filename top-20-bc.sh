#!/bin/bash

# get the weather data for the top 20 stations in BC
debug=0
# List of Station IDs
station_ids=(568 707 271 1364 244 68 97 1340 1180 87 26 65 262 1142 888 1039 1032 925 358 1056)
# station_ids=(568)

# rather than have complex loop break logic I'm going to write three loops, one for each timeframe

# 1 = hourly
timeframe=1
echo "Processing Timeframe: ${timeframe}"

# Iterate through the Station IDs and run the wget command
for station_id in "${station_ids[@]}"; do
  echo "Processing Station ID: $station_id"
  for year in `seq 1888 2023`; do
    # for year in `seq 2022 2023`; do
        echo "Processing Year: $year"
        for month in `seq 1 12`; do
        # for month in `seq 1 2`; do
            echo "Processing Month: $month"
                # make a directory for the station
                mkdir -p "data"/$station_id
                # make a directory for the year inside the station directory
                mkdir -p "data"/$station_id/$year
                # make a directory for the month inside the year directory
                mkdir -p "data"/$station_id/$year/$month

                # download the data into the timeframe directory

                wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P "data"/$station_id/$year/$month;
                # wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&time=utc&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P $station_id/$year/$month/$timeframe;
                # wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=1&submit=Download+Data"
                
                echo "Downloaded hourly data for Station ID: $station_id, Year: $year, Month: $month"
        done;
    done;
done

# 2 = daily

timeframe=2
month=1

echo "Processing Timeframe: $timeframe"

for station_id in "${station_ids[@]}"; do
  echo "Processing Station ID: $station_id"
#   for year in `seq 1888 2023`; do
    # for year in `seq 2021 2023`; do
        echo "Processing Year: $year"
        # make a directory for the station
        mkdir -p "data"/$station_id
        # make a directory for the year inside the station directory
        mkdir -p "data"/$station_id/$year

        # download the data into the timeframe directory

        wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P "data"/$station_id/$year;
        # wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&time=utc&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P $station_id/$year/$month/$timeframe;
                
        echo "Downloaded Daily data for Station ID: $station_id, Year: $year"
    done;
done

# 3 = monthly

timeframe=3
month=1

echo "Processing Timeframe: $timeframe"

# Iterate through the Station IDs and run the wget command
for station_id in "${station_ids[@]}"; do
  echo "Processing Station ID: $station_id"
  for year in `seq 1888 2023`; do
    # for year in `seq 2021 2023`; do
        echo "Processing Year: $year"
        # make a directory for the station
        mkdir -p "data"/$station_id
        # make a directory for the year inside the station directory
        mkdir -p "data"/$station_id/$year
        # download the data into the timeframe directory

        wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=${station_id}&Year=${year}&timeframe=${timeframe}&submit=Download+Data" -P "data"/$station_id/$year;
        # wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&time=utc&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P $station_id/$year/$month/$timeframe;
        
        echo "Downloaded monthly data for Station ID: $station_id, Year: $year"
    done;
done

echo "All stations processed."

