# Tickers DB 

## Ingesting Historical Data from CSVs 
Assuming you have csv file containing data for each ticker, this data will be ingested into Postgresql DB. 


## Ingest Last-Day Data from JSON
Assuming you have last-day data in .json file, this data will be ingested into the historical data of the DB. 


├── project_name/
│   ├── data/
│   │   ├── csv_files/
│   │   └── sql_scripts/
│   ├── src/
│   │   ├── database.py
│   │   ├── initializer.py
│   │   ├── main.py
│   ├── requirements.txt
│   ├── config.ini
