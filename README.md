# DC Traffic Enforcement Analytics
### ETL Pipeline · Relational Database · SQL Analysis

![Python](https://img.shields.io/badge/Python-3.8+-blue)
![MySQL](https://img.shields.io/badge/MySQL-8.0-orange)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-green)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

---

## Background

DC traffic violation records and weather data exist 
in separate public sources with no shared 
infrastructure for cross-analysis. Enforcement 
patterns, weather impact on violations, and agency 
performance cannot be analyzed without first 
building a unified data layer.

This project builds that layer — from raw CSV 
ingestion to a normalized relational database — 
and runs operational SQL analysis to surface 
actionable enforcement insights.

---

## What's Inside
```
dc-traffic-enforcement-analytics/
├── etl/
│   ├── moving_violations_etl.py   
│   ├── weather_etl.py             
│   └── README.md                  
├── sql/
│   ├── 01_create_schema.sql       
│   ├── 02_analysis_queries.sql    
│   └── README.md                  
├── docs/
│   ├── project_documentation.pdf
│   └── screenshots/
│       ├── ERD_Diagram.png
│       ├── database_tables.png
│       └── record_counts.png
├── data/
│   └── README.md                  
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Database Design

![ERD](docs/screenshots/ERD_Diagram.png)

Two normalized tables linked by date:

| Table | What it holds |
|-------|--------------|
| moving_violations | Every ticket issued — location, agency, violation type, fine, accident flag |
| weather_dc | Hourly DC weather — temperature, rain, wind, UV, conditions |

---
## Pipeline Architecture
```
DC Open Data Portal       Visual Crossing
(Moving Violations CSV)   (Weather CSV)
        │                       │
        ▼                       ▼
moving_violations_etl.py  weather_etl.py
        │                       │
        ├── Column standardization
        ├── Type conversion
        ├── Null handling
        ├── Deduplication
        └──────────┬────────────┘
                   ▼
          MySQL Database
           (dc_traffic)
                   │
                   ▼
       SQL Analysis Layer
```

---

## ETL Pipeline Details

### Moving Violations
| Parameter | Detail |
|-----------|--------|
| Source | DC Open Data Portal |
| Processing | Chunked — 50,000 rows per chunk |
| Fields retained | 28 columns |
| Key transformations | Date parsing, HHMM time conversion, boolean mapping, numeric coercion, deduplication |
| Output table | moving_violations |

### Weather Data
| Parameter | Detail |
|-----------|--------|
| Source | Visual Crossing (monthly CSVs) |
| Key transformations | Column renaming (23 fields), null fills, datetime parsing, deduplication |
| Output table | weather_dc |

---
---

## Analysis

Eight SQL queries across four themes:

**Agency Performance**
Which agencies issue the most tickets, and how does 
that change month to month?

**Temporal Patterns**
When do violations peak — by day of week and hour of day?

**Weather Impact**
Do violation rates drop when it rains? 
Do accidents cluster in wet or dry conditions?

**Financial Trends**
Where does fine revenue concentrate, and which 
violation types drive high-severity enforcement?

---


## Key Findings

- Violations peaked **mid-week** during 
  **afternoon hours**
- Violation rates were **higher during non-rainy 
  conditions** — weather does not appear to 
  deter violations
- Accident-related tickets concentrated in 
  **non-rainy conditions**, suggesting 
  overconfidence in dry weather
- High-severity speeding fines showed consistent 
  monthly patterns with identifiable 
  agency concentration

---

## Screenshots

**Database Tables Loaded**
![Tables](docs/screenshots/database_tables.png)

**Record Counts**
![Records](docs/screenshots/record_counts.png)

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.8+ | ETL scripting |
| Pandas | Transformation and cleaning |
| SQLAlchemy | ORM and database connection |
| MySQL 8.0 | Relational database |
| python-dotenv | Secure credential management |
| SQL | Operational analysis |

---

## Setup

### 1. Install MySQL

Download MySQL Community Server:
🔗 https://dev.mysql.com/downloads/mysql/

Once installed, open Workbench, connect, 
open a new query tab, and run:
```sql
CREATE DATABASE dc_traffic;
```

Confirm `dc_traffic` appears in the left 
panel under Schemas.

---

### 2. Get the Data

Raw files are not included in this repo — both 
datasets are publicly available and free.
See [data/README.md](data/README.md) for 
download links and exact folder structure required.

---

### 3. Create Your .env File

In the project root, create a file named `.env` 
(no extension) containing:
```
DB_PASSWORD=your_mysql_root_password
```
---

### 4. Install Dependencies
```bash
pip install -r requirements.txt
```
Or install manually:
```bash
pip install pandas sqlalchemy pymysql python-dotenv cryptography
````

Verify Python version first:
```bash
python --version
```

Should return 3.8 or higher.

---

### 5. Confirm Folder Structure

Before running, your project root must contain:
```
Violations Data/     ← moving violations CSV files
Weather Data/        ← monthly weather CSV files
.env                 ← your DB_PASSWORD
```

---

### 6. Run the Pipelines
```bash
# Weather first — violations join against it
python etl/weather_etl.py

# Then violations
python etl/moving_violations_etl.py
```

---

### 7. Run the SQL Analysis

Open MySQL Workbench:
1. File → Open SQL Script → `sql/01_create_schema.sql` 
   → run with ⚡
2. Open `sql/02_analysis_queries.sql`
3. Run queries individually with 
   `Ctrl+Enter` (Windows) or `Cmd+Return` (Mac)

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `No module named 'dotenv'` | `pip install python-dotenv` |
| `Access denied for user 'root'` | Check DB_PASSWORD in .env |
| MySQL won't connect | Open Workbench and verify the connection first |
| `FileNotFoundError: Violations Data` | Folder name must match exactly, including the space |
| `Table doesn't exist` | Run weather ETL before violations ETL |

---

## Author

**Adarsh Shukla**
MS Business Analytics · University of Dayton
[LinkedIn](https://linkedin.com/in/adarshhshukla) · 
[GitHub](https://github.com/alsoadarsh)
