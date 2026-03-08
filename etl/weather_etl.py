import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import Integer, Float, String, DateTime
import os
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")

engine = create_engine(
    f"mysql+pymysql://root:{DB_PASSWORD}@127.0.0.1/dc_traffic",
    echo=False
)

base_path = os.path.dirname(os.path.abspath(__file__))

weather_folder = os.path.join(base_path, "Weather Data")

csv_files = [f for f in os.listdir(weather_folder) if f.endswith(".csv")]

monthly_data = []

for file in csv_files:
    full_path = os.path.join(weather_folder, file)
    print(f"Importing: {full_path}")
    temp_df = pd.read_csv(full_path)
    monthly_data.append(temp_df)

df_raw = pd.concat(monthly_data, ignore_index=True)

df_raw.columns = df_raw.columns.str.lower().str.replace(" ", "_")

selected_cols = df_raw.filter(
    items=[
        "datetime", "temp", "feelslike", "dew", "humidity",
        "precip", "precipprob", "preciptype",
        "snow", "snowdepth",
        "windgust", "windspeed", "winddir",
        "sealevelpressure", "cloudcover", "visibility",
        "solarradiation", "solarenergy", "uvindex", "severerisk",
        "conditions", "icon", "stations"
    ]
)

rename_map = [
    ("datetime", "date_time"), ("temp", "temperature"),
    ("feelslike", "feels"), ("dew", "dew_point"),
    ("humidity", "humidity"), ("precip", "rain"),
    ("precipprob", "rain_prob"), ("preciptype", "rain_type"),
    ("snow", "snow"), ("snowdepth", "snow_depth"),
    ("windgust", "winds"), ("windspeed", "wind_speed"),
    ("winddir", "wind_direction"), ("sealevelpressure", "pressure"),
    ("cloudcover", "clouds"), ("visibility", "visibility"),
    ("solarradiation", "sunlight"), ("solarenergy", "solar_energy"),
    ("uvindex", "uv"), ("severerisk", "risk"),
    ("conditions", "condition"), ("icon", "icon"), ("stations", "station")
]

for old, new in rename_map:
    selected_cols.rename(columns={old: new}, inplace=True)

df_trimmed = selected_cols.copy()

df_trimmed['date_time'] = pd.to_datetime(df_trimmed['date_time'], errors='coerce')
df_trimmed = df_trimmed.dropna(subset=['date_time'])

fill_values = {
    'rain_type': 'none',
    'risk': 0,
    'snow': 0,
    'snow_depth': 0,
    'station': 'na'
}

df_trimmed = df_trimmed.fillna(fill_values)

df_clean = df_trimmed.drop_duplicates()

print("Cleaned weather data ready for loading.")

metadata = MetaData()

cols = [
    Column("record_id", Integer, primary_key=True, autoincrement=True),
    Column("date_time", DateTime),
    Column("temperature", Float),
    Column("feels", Float),
    Column("dew_point", Float),
    Column("humidity", Float),
    Column("rain", Float),
    Column("rain_prob", Float),
    Column("rain_type", String(40)),
    Column("snow", Float),
    Column("snow_depth", Float),
    Column("winds", Float),
    Column("wind_speed", Float),
    Column("wind_direction", Float),
    Column("pressure", Float),
    Column("clouds", Float),
    Column("visibility", Float),
    Column("sunlight", Float),
    Column("solar_energy", Float),
    Column("uv", Float),
    Column("risk", Float),
    Column("condition", String(200)),
    Column("icon", String(80)),
    Column("station", String(150))
]

weather_table = Table("weather_dc", metadata, *cols)

metadata.create_all(engine)
print("Table created successfully.")

print("Starting data insert into database...")
df_clean.to_sql(
    name="weather_dc",
    con=engine,
    if_exists="append",
    index=False,
    chunksize=300
)
print("Data inserted successfully.")

row_check = pd.read_sql("SELECT COUNT(*) AS total_records FROM weather_dc", con=engine)
print(row_check)

print("Weather ETL process finished successfully.")
