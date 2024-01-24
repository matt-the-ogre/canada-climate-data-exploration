# canada-climate-data-exploration

Downloading and exploring Canadian Climate Data

From the Canada gov site:

[Support files are here.](https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/)

```bash
for year in `seq 1998 2008`;do for month in `seq 1 12`;do wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=1706&Year=${year}&Month=${month}&Day=14&timeframe=1&submit= Download+Data" ;done;done
```

[Download latest stations list here.](https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/Station%20Inventory%20EN.csv)

```bash
wget "https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/Station%20Inventory%20EN.csv"
mv "Station Inventory EN.csv" "data/stations.csv"
# remove the first three lines of metadata to make it a pure CSV file
sed -i '.bak' '1,3d' data/stations.csv
```

Get the stations in BC that have hourly data in 2023:

```bash
python3 print-current-stations-bc-hourly.py > current-stations-bc-hourly.csv
```

Find the last data point with not null data quality:

```sql
SELECT "Year", "Month", "Day"
FROM "1011500_26_daily" WHERE "Data Quality" IS NOT NULL
ORDER BY "Date/Time" DESC
LIMIT 1;
```

Interesting that on August 22, 2023 the last data point with not null data quality is June 27, 2023. How often are these CSV files updated?

Find the list of stations in BC with a Daily First Year not null

```sql
SELECT "Station ID" FROM stations
WHERE "Province" = 'BRITISH COLUMBIA'
AND "DLY First Year" IS NOT NULL;
```

## Getting started

```bash
pip3 install -r ./update-database-daily/requirements.txt
```

### Download and import the stations list

This will download the latest stations list as a CSV file and import it into the local SQLite database and the remote MySQL database.

```bash
cd scripts
python3 import_stations.py
```

### Data files

If you have already downloaded many or all of the CSV files put them in the `/data/` directory with sub-directories by station ID.

## More from the Climate website

[Source file](https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/Command_Lines_EN.txt)

Hourly data interval:

```bash
for year in `seq 1998 2008`;do for month in `seq 1 12`;do wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=1706&Year=${year}&Month=${month}&Day=14&timeframe=1&submit= Download+Data" ;done;done
```

Note: to download the hourly data in UTC rather than LTC, please add &time=utc after format=csv in the wget statement

Daily data interval:

```bash
for year in `seq 1998 2008`;do for month in `seq 1 1`;do wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=1706&Year=${year}&Month=${month}&Day=14&timeframe=2&submit= Download+Data" ;done;done
```

Monthly data interval (specific time period):

```bash
for year in `seq 1998 2008`;do for month in `seq 1 12`;do wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=1706&Year=${year}&Month=${month}&Day=14&timeframe=3&submit= Download+Data" ;done;done
```

Monthly data interval (station complete history):

```bash
for year in `seq 1998 2008`;do for month in `seq 1 1`;do wget --content-disposition "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=1706&Year=${year}&Month=${month}&Day=14&timeframe=3&submit= Download+Data" ;done;done
```

## Glossary from Government of Canada

[Glossary page](https://climate.weather.gc.ca/glossary_e.html)

Transport Canada Identifier (TC ID)

The TC ID is the identifier assigned by Transport Canada to identify meteorological reports from airport observing sites transmitted in real time in aviation formats.

World Meteorological Organization Identifier (WMO ID)

A 5-digit number permanently assigned to Canadian stations by the World Meteorological Organization to identify the station internationally. The WMO ID is an international identifier assigned by the Meteorological Service of Canada to standards of the World Meteorological Organization for stations that transmit observations in international meteorological formats in real time.

## Thoughts on performance

I should set up the local sqlite database again just for the stations data. Then the station_id to climate_id lookup would be much faster and I wouldn't have to query the cloud database for that.

I should also look up the climate_id from the station_id once per station and pass it as a parameter into any repetitive function to avoid the lookup in the inner loop.

## Update in 2024

So the station list CSV hasn't been updated since early 2023. I wonder if this is still a good source of data?

I was poking around the Canadian Weather site and found this:

[ClimateData.ca](https://climatedata.ca/download/#station-download)

Let's look at their API format using this sample call:

`https://api.weather.gc.ca/collections/climate-daily/items?datetime=1840-03-01%2000:00:00/2024-01-23%2000:00:00&STN_ID=758|762|764|788|850|875|876|687|877|862|926|925|927|928&sortby=PROVINCE_CODE,STN_ID,LOCAL_DATE&f=csv&limit=150000&startindex=0`

Base URL: `https://api.weather.gc.ca/`
Route: `collections/climate-daily/items`
Parameters:
datetime: appears to be starting YYYY-MM-DD HH:MM:SS and ending YYYY-MM-DD HH:MM:SS
STN_ID: 758 for example. I wonder if this is the same list of station IDs from the Get More Data site?
sortby: PROVINCE_CODE,STN_ID,LOCAL_DATES
f: presumably format. CSV or maybe XML? Or the GeoJSON format? Yes. `json` is a valid option.
limit: integer. Number of records maybe?
startindex: integer. Used to paginate through the data maybe?

What would a `wget` or `curl` call look like for this?

Use STN_ID=203 for "PORT HARDY BEAVER HR"

```bash
curl -OJL "https://api.weather.gc.ca/collections/climate-daily/items?datetime=1840-03-01%2000%3A00%3A00%2F2024-01-23%2000%3A00%3A00&STN_ID=203&sortby=PROVINCE_CODE,STN_ID,LOCAL_DATE&f=csv&limit=150000&startindex=0"
```

Interesting! It downloaded a file called `climate-daily.csv` with a different format than the other site.

### More new interesting stuff

There are directories full of CSV files available for download here:

[Climate Observations](https://dd.weather.gc.ca/climate/observations/)

There's a file in there called `climate_station_list.csv` that looks up to date.

So instead of using convoluted web call I could just synchronize this folder?

If you've already done a bulk download and just want to grab newer files on subsequent runs, you can use `wget` with the `-N` (timestamping) option. This makes `wget` download only files that are newer than the ones you already have. Here's how you'd modify the command:

```bash
wget -r -l1 -N -A.csv -np -erobots=off https://dd.weather.gc.ca/climate/observations/daily/csv/BC/
```

What this does:
- `-r` and `-l1`: As before, it's recursive but only one level deep.
- `-N`: Timestamping. `wget` will check the timestamp of the remote file against the local file and only download if the remote file is newer.
- `-A.csv`: Still focusing only on `.csv` files.
- `-np`: Prevents ascending to the parent directory.
- `-erobots=off`: Ignores robots.txt (same caution applies about compliance with site policies).

This way, each time you run this command, it will only download files that have been updated since your last download.