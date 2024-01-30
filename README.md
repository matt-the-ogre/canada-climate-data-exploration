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

Here's another link with `CLIMATE_IDENTIFIER` instead of `STN_ID`:

`https://api.weather.gc.ca/collections/climate-daily/items?datetime=1840-03-01%2000:00:00/2024-01-24%2000:00:00&CLIMATE_IDENTIFIER=1026271&sortby=PROVINCE_CODE,CLIMATE_IDENTIFIER,LOCAL_DATE&f=csv&limit=10000&offset=0`

Changed `startindex` to `offset` too.

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

Ah, I see. If the `wget` command is downloading the HTML instead of the individual files, it's likely because the files are listed on an HTML page rather than being available directly in the directory structure. To download all the `.csv` files linked from that page, you'll want to modify the command slightly:

```bash
wget -r -l1 -H -nd -N -A.csv -np -e robots=off -Ddd.weather.gc.ca https://dd.weather.gc.ca/climate/observations/daily/csv/BC/
```

Here's what's changed:

- `-r`: Recursive download.
- `-l1`: Depth of 1 level.
- `-H`: Span hosts, since the files might be located on a different host.
- `-nd`: Download files to the current directory.
- `-N`: Timestamping.
- `-A.csv`: Accept only `.csv` files.
- `-np`: Do not follow links to parent directories.
- `-e robots=off`: Ignore `robots.txt`.
- `-Ddd.weather.gc.ca`: Restrict the download to the specified domain (`dd.weather.gc.ca` in this case).

This should download all the `.csv` files linked on that page. Just make sure to adjust the domain (`-D`) option if the files are hosted on a different domain.

Neither of the above worked.

It did download the HTML file for that page. I suppose I just need to format and extract all the filenames from there and try that?

More:

To bulk download files from a Web Accessible Folder (WAF) on your Mac, you can use command-line tools like `wget` or `curl`. Since `wget` isn't installed by default on macOS, you might prefer using `curl`, which is included in macOS. However, if you're comfortable installing new software, `wget` can be more convenient for this task.

### Using `wget`:

If you decide to use `wget`, you can install it via Homebrew:

1. Install Homebrew if you haven't already: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
2. Install `wget`: `brew install wget`

Then, use a command similar to:

```bash
wget -r -np -nH --cut-dirs=1 -A 'pattern*' http://example.com/directory/
wget -r -np -nH --cut-dirs=1 -A '*.csv' https://dd.weather.gc.ca/climate/observations/daily/csv/BC/
```

- `-r`: Recursive download
- `-np`: Don't ascend to the parent directory
- `-nH`: Disable generation of host-prefixed directories
- `--cut-dirs=1`: Ignore certain directory components (this number varies based on URL structure)
- `-A 'pattern*'`: Download files with a specific pattern (e.g., `.csv`, `.jpg`)

Replace `pattern*` with the file type you want to download (like `*.csv`) and `http://example.com/directory/` with your WAF URL.

### Using `curl`:

If you prefer `curl`, which is more manual and works well for downloading specific files:

1. Create a text file (`filelist.txt`) that contains all the URLs of the files you want to download.
2. Use a command like:

```bash
xargs -n 1 curl -O < filelist.txt
```

This reads each URL from `filelist.txt` and uses `curl` to download it.

To split a large file into smaller files with a specific number of lines each, you can use the `split` command on your Mac. For your case, where you want to split a file into chunks of 1000 lines, the command would look like this:

```bash
split -l 1000 output.txt split_
```

Here's what this does:

- `-l 1000`: Specifies the number of lines per output file.
- `yourfile.txt`: The name of your large file.
- `newfile_`: The prefix for the output files. The command will create files named `newfile_aa`, `newfile_ab`, `newfile_ac`, and so on.

After running this command, you'll have multiple files each containing 1000 lines from the original file.

Yes, Matt, you can add a pause in the `xargs` command for `curl`. One way to do this is by using the `sleep` command in a shell script. Here's how you can modify the `xargs` command to include a pause:

```bash
xargs -n 1 -I {} sh -c 'curl -O "{}"; sleep 1' < ../split_aa
```

In this command:

- `-n 1`: This tells `xargs` to use one line at a time from `filelist.txt`.
- `-I {}`: This replaces `{}` with the line from `filelist.txt`.
- `sh -c`: Executes the given command in the shell.
- `curl -O "{}"`: Downloads the file from the URL.
- `sleep 1`: Pauses for 1 second after each download.

You can adjust the `sleep 1` part to increase or decrease the pause duration as needed (it's in seconds). For example, `sleep 2` would pause for 2 seconds between each download.

## Updated data downloading steps

1. download HTML page for each province
1. clean that html to extract just the links to the CSV files referenced
1. Add the prefix (URL) to all those links to prepare it for curl to download them
1. split the file into sub-files with 1000 lines each. This is to prepare to download the files and I don't trust more than 1000 files per directory
1. run the xargs command with curl with each filelist as input
1. Profit

mkdir ae
cd ae
xargs -n 1 curl -O < ../split_ae

Just did a quick read on number of files in a directory on MacOS and it said 2.1 billion. So we might be okay.
