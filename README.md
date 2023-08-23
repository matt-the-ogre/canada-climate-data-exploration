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
