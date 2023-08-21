#!/bin/bash

# get the weather data for the top 20 stations in BC

# parameters are station id, year, and timeframe

debug=0


# List of Station IDs

# if debug is 1, then just run the first station
# if debug is 0, then run all stations in the list or the station id passed in as a parameter
if [ "$debug" -eq 0 ]; then
  echo "Debug is 0, running normal command."
  # station_ids_complete=(568 707 271 1364 244 68 97 1340 1180 87 26 65 262 1142 888 1039 1032 925 358 1056)
  # station_ids="${1:-${station_ids_complete}}"
  # if parameter 1 is not passed in, then use the complete list
  if [ -z "$1" ]; then
    # station_ids="${station_ids_complete}"
    station_ids=(568 707 271 1364 244 68 97 1340 1180 87 26 65 262 1142 888 1039 1032 925 358 1056)
  else
    # if parameter 1 is passed in, then use that station id
    station_ids="${1}"
  fi
else
  echo "Debug is 1, running with just the first station, id = 568."
  station_ids=(568)
fi
# echo "Station ID list is: ${station_ids[@]}"
# declare -p station_ids
# echo "Station ID complete list is: $station_ids_complete"
# echo "Station ID @ complete list is: ${station_ids_complete[@]}"
# declare -p station_ids_complete
# declare -p station_ids_complete[@]

# exit 1;

# year is the second parameter and defaults to 2023
year="${2:-2023}"
echo "Year is: $year"
# timeframe is the third parameter and defaults to 3 (monthly)
timeframe="${3:-3}"
echo "Timeframe is: $timeframe"

# rather than have complex loop break logic I'm going to write three loops, one for each timeframe
if [ "$timeframe" -eq 1 ]; then
  echo "Timeframe is 1, hourly."
  echo "Processing Timeframe: ${timeframe}"

  # Iterate through the Station IDs and run the wget command
  for station_id in "${station_ids[@]}"; do
    echo "Processing Station ID: $station_id"
    sleep 0.5
    for year in `seq 1888 2023`; do
          echo "Processing Year: $year"
          for month in `seq 1 12`; do
              echo "Processing Month: $month"
                  # make a directory for the station
                  mkdir -p "data"/$station_id
                  # make a directory for the year inside the station directory
                  mkdir -p "data"/$station_id/$year
                  # make a directory for the month inside the year directory
                  mkdir -p "data"/$station_id/$year/$month

                  # download the data into the timeframe directory

                  wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P "data"/$station_id/$year/$month;

                  echo "Downloaded hourly data for Station ID: $station_id, Year: $year, Month: $month"
          done;
      done;
  done
elif [ "$timeframe" -eq 2 ]; then
  echo "Timeframe is 2, daily."
  month=1
  echo "Processing Timeframe: $timeframe"

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

          wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P "data"/$station_id/$year;
          # wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&time=utc&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P $station_id/$year/$month/$timeframe;
                  
          echo "Downloaded Daily data for Station ID: $station_id, Year: $year"
      done;
  done

elif [ "$timeframe" -eq 3 ]; then
  # turns out you don't have to pass the year to this URL
  # it will download all the monthly data for all the years for this station
  echo "Timeframe is 3, monthly."
  # month=1
  echo "Processing Timeframe: $timeframe"

  # Iterate through the Station IDs and run the wget command
  for station_id in "${station_ids[@]}"; do
    echo "Processing Station ID: $station_id"
    # for year in `seq 1888 2023`; do
      # for year in `seq 2021 2023`; do
          # echo "Processing Year: $year"
          # make a directory for the station
          mkdir -p "data"/$station_id
          # make a directory for the year inside the station directory
          # mkdir -p "data"/$station_id/$year
          # download the data into the timeframe directory

          wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=${station_id}&timeframe=${timeframe}&submit=Download+Data" -P "data"/$station_id;
          # wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&time=utc&stationID=${station_id}&Year=${year}&Month=${month}&Day=14&timeframe=${timeframe}&submit=Download+Data" -P $station_id/$year/$month/$timeframe;
          
          echo "Downloaded monthly data for Station ID: $station_id, Year: $year"
      # done;
  done
else
  echo "Timeframe is not 1, 2, or 3, exiting."
  exit 1
fi

# 2 = daily

# timeframe=2

# 3 = monthly



echo "All stations processed."

