import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import Integer, Float, String, Date, Time, DateTime, Boolean
import os
import datetime
from dotenv import load_dotenv

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")

engine = create_engine(
    f"mysql+pymysql://root:{DB_PASSWORD}@127.0.0.1/dc_traffic",
    echo=False
)

base_path = os.path.dirname(os.path.abspath(__file__))
mv_folder = os.path.join(base_path, "Violations Data")
csv_files = [f for f in os.listdir(mv_folder) if f.endswith(".csv")]

chunk_size = 50000
processed_chunks = []

for file in csv_files:
    full_path = os.path.join(mv_folder, file)
    print(f"Importing (chunked): {full_path}")

    for chunk in pd.read_csv(
        full_path,
        dtype=str,
        chunksize=chunk_size,
        engine="python",
        on_bad_lines="skip"
    ):


        chunk.columns = chunk.columns.str.lower().str.replace(" ", "_")

        rename_map = [
            ("objectid", "ticket_id"),
            ("violation_process_desc", "violation_desc"),
            ("accident_indicator", "accident"),
            ("gis_last_mod_dttm", "last_modified")
        ]
        for old, new in rename_map:
            chunk.rename(columns={old: new}, inplace=True)

        selected = chunk.filter(items=[
            "ticket_id", "location", "xcoord", "ycoord",
            "issue_date", "issue_time",
            "issuing_agency_code", "issuing_agency_name", "issuing_agency_short",
            "violation_code", "violation_desc",
            "plate_state", "accident",
            "disposition_code", "disposition_type", "disposition_date",
            "fine_amount", "total_paid",
            "penalty_1", "penalty_2", "penalty_3", "penalty_4", "penalty_5",
            "rp_mult_owner_no", "body_style",
            "latitude", "longitude",
            "mar_id", "last_modified"
        ])

        ticket_ids = pd.to_numeric(selected["ticket_id"], errors="coerce")
        selected["ticket_id"] = ticket_ids
        selected = selected[ticket_ids.notna()]

        selected["issue_date"] = pd.to_datetime(selected["issue_date"], errors="coerce")
        selected["disposition_date"] = pd.to_datetime(selected["disposition_date"], errors="coerce")

        selected["last_modified"] = (
            pd.to_datetime(selected["last_modified"], errors="coerce")
            if "last_modified" in selected.columns else None
        )

        def convert_time(t):
            try:
                s = f"{t:0>4}"
                h, m = int(s[:2]), int(s[2:])
                return datetime.time(h, m)
            except:
                return None

        selected["issue_time"] = selected["issue_time"].apply(convert_time)

        selected["accident"] = selected["accident"].map(
            lambda v: 1 if str(v).upper().startswith(("Y", "1")) else 0
        )

        numeric_fields = [
            "fine_amount", "total_paid",
            "penalty_1", "penalty_2", "penalty_3", "penalty_4", "penalty_5",
            "latitude", "longitude"
        ]

        for col in numeric_fields:
            if col in selected:
                selected[col] = pd.to_numeric(selected[col], errors="coerce")

        selected = selected.drop_duplicates()

        processed_chunks.append(selected)

print("All chunks cleaned and ready for loading.")

metadata = MetaData()

cols = [
    Column("ticket_id", Integer, primary_key=True, autoincrement=True),
    Column("location", String(255)),
    Column("xcoord", String(50)),
    Column("ycoord", String(50)),
    Column("issue_date", Date),
    Column("issue_time", Time),
    Column("issuing_agency_code", String(20)),
    Column("issuing_agency_name", String(150)),
    Column("issuing_agency_short", String(40)),
    Column("violation_code", String(20)),
    Column("violation_desc", String(300)),
    Column("plate_state", String(10)),
    Column("accident", Boolean),
    Column("disposition_code", String(20)),
    Column("disposition_type", String(50)),
    Column("disposition_date", Date),
    Column("fine_amount", Float),
    Column("total_paid", Float),
    Column("penalty_1", Float),
    Column("penalty_2", Float),
    Column("penalty_3", Float),
    Column("penalty_4", Float),
    Column("penalty_5", Float),
    Column("rp_mult_owner_no", String(50)),
    Column("body_style", String(50)),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("mar_id", String(50)),
    Column("last_modified", DateTime)
]

mv_table = Table("moving_violations", metadata, *cols)
metadata.create_all(engine)

print("Table 'moving_violations' created successfully.")

print("Starting insert into database...")

for i, chunk in enumerate(processed_chunks):
    print(f"Inserting chunk {i + 1} of {len(processed_chunks)} ...")
    chunk.to_sql(
        name="moving_violations",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=500
    )

print("All chunks inserted successfully.")

row_check = pd.read_sql("SELECT COUNT(*) AS total_records FROM moving_violations", con=engine)
print(row_check)

print("Moving Violations ETL process finished successfully.")
