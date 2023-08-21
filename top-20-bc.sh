#!/bin/bash

# get the weather data for the top 20 stations in BC

# parameters are station id, year, and timeframe

debug=0

# List of Station IDs in BC with the longest history: (568 707 271 1364 244 68 97 1340 1180 87 26 65 262 1142 888 1039 1032 925 358 1056)

# if debug is 0, then run all stations in the list or the station id passed in as a parameter
# if debug is 1, then just run the first station
if [ "$debug" -eq 0 ]; then
  # echo "Debug is 0, running normal command."
  # if parameter 1 is not passed in, then use the complete list
  if [ -z "$1" ]; then
    station_ids=(568 707 271 1364 244 68 97 1340 1180 87 26 65 262 1142 888 1039 1032 925 358 1056)
  else
    # if parameter 1 is passed in, then use that station id
    station_ids="${1}"
  fi
else
  echo "Debug is 1, running with just the first station, id = 568."
  station_ids=(568)
fi

# year is the second parameter and defaults to 2023
year="${2:-2023}"
if [ "$debug" ]; then echo "Year is: $year"; fi;

# timeframe is the third parameter and defaults to 3 (monthly)
timeframe="${3:-3}"
if [ "$debug" ]; then echo "Timeframe is: $timeframe"; fi;

# rather than have complex loop break logic I'm going to write three loops, one for each timeframe
if [ "$timeframe" -eq 1 ]; then
  echo "Timeframe is 1, hourly."

  # Iterate through the Station IDs and run the wget command
  for station_id in "${station_ids[@]}"; do
    echo "Processing Station ID: $station_id"
    for year in `seq 1888 2023`; do
          echo "Processing Year: $year"
          for month in `seq 1 12`; do
              echo "Processing Month: $month"
              sleep 0.5 # sleep for half a second to avoid overloading the server
              # make a directory for the station
              mkdir -p "data"/$station_id
              # make a directory for the year inside the station directory
              mkdir -p "data"/$station_id/$year
              # make a directory for the month inside the year directory
              mkdir -p "data"/$station_id/$year/$month

              # download the data into the timeframe directory

              wget --quiet --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P "data"/$station_id/$year/$month;

              echo "Downloaded hourly data for Station ID: $station_id, Year: $year, Month: $month"
          done;
      done;
  done
elif [ "$timeframe" -eq 2 ]; then
  echo "Timeframe is 2, daily."
  month=1

  for station_id in "${station_ids[@]}"; do
      echo "Processing Station ID: $station_id"
      for year in `seq 1888 2023`; do
          # for year in `seq 2021 2023`; do
          echo "Processing Year: $year"
          sleep 0.5 # sleep for half a second to avoid overloading the server
          # make a directory for the station
          mkdir -p "data"/$station_id
          # make a directory for the year inside the station directory
          mkdir -p "data"/$station_id/$year

          # download the data into the year directory

          wget --quiet --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P "data"/$station_id/$year;
                  
          echo "Downloaded Daily data for Station ID: $station_id, Year: $year"
      done;
  done

elif [ "$timeframe" -eq 3 ]; then
  # turns out you don't have to pass the year to this URL
  # it will download all the monthly data for all the years for this station
  echo "Timeframe is 3, monthly."

  # Iterate through the Station IDs and run the wget command
  for station_id in "${station_ids[@]}"; do
    echo "Processing Station ID: $station_id"
    sleep 0.5 # sleep for half a second to avoid overloading the server
          # make a directory for the station
          mkdir -p "data"/$station_id

          # download the data into the station id directory

          wget --quiet --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=${station_id}&timeframe=${timeframe}&submit=Download+Data" -P "data"/$station_id;
          
          echo "Downloaded monthly data for Station ID: $station_id"
      # done;
  done
else
  echo "Timeframe is not 1, 2, or 3, exiting."
  exit 1
fi

echo "All stations processed."
