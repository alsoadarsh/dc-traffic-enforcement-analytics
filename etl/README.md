## ETL Pipelines

### weather_etl.py

Reads monthly weather CSV files from the `Weather Data/` 
folder, standardizes 23 column names, fills structural 
nulls (rain type, snow depth, risk score), parses 
datetime fields, deduplicates, and loads into 
the `weather_dc` table in MySQL.

Run this first. The violations pipeline joins against 
weather on date.

### moving_violations_etl.py

Reads DC moving violations CSV files from the 
`Violations Data/` folder in chunks of 50,000 rows. 
For each chunk: standardizes column names, renames 
four fields, selects 28 relevant columns, parses 
issue dates and disposition dates, converts HHMM 
integer times to proper time objects, maps accident 
indicators to boolean, coerces nine numeric fields, 
and deduplicates. All chunks are collected and 
inserted together into the `moving_violations` table.
