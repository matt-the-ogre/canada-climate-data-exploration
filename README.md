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
