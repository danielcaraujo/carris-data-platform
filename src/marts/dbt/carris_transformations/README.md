# Carris Metropolitana - dbt Transformations

This **dbt project** is part of the data platform for analyzing public transportation operated by Carris Metropolitana, the main bus operator in the Lisbon metropolitan area. It focuses on the **transformation and modeling** of raw and staged data into clean, analytics-ready models.

## Project Overview

The main goal of this project is to:
- Extract data from the [Carris Metropolitana API](https://api.carrismetropolitana.pt/v2/) and GTFS feeds
- Load and process it into BigQuery using PySpark
- Apply dimensional modeling and business logic with dbt, preparing the data for analytical use cases

This README refers only to the **dbt project** directory, which handles the final transformation and modeling of the data.

## Data Sources

The datasets used in this dbt project are sourced from both the Carris API endpoints and the GTFS (General Transit Feed Specification) files, including:
- **Stops** (`stops`)
- **Routes** (`routes`)
- **Trips** (`trips`)
- **Shapes** (`shapes`)
- **Stop Times** (`stop_times`)
- **Calendar Dates** (`calendar_dates`)
- **Dates** (`dates`)
- **Periods** (`periods`)
- **Municipalities and Regions** (`municipalities`)

These are integrated to provide a unified view of public transport activity in the Lisbon metropolitan area.

## Data Modeling

We follow dimensional modeling best practices, separating data into **dimension** and **fact** tables to optimize for analytical queries.

### dbt Layers

- **Staging**: `stg_*` models clean and standardize raw/staged BigQuery data (naming conventions, data types, initial transformations)
- **Marts**: Analytical models, already aggregated and enriched, optimized for business analysis (`dim_*` and `fact_*`)
- **Snapshots**: Time-travel enabled models tracking changes to dimensions over time, allowing historical analysis.

#### Main Tables

**Dimensions:**
- `dim_stop`: Detailed information on each stop (location, municipality, region, proximity to schools, etc.)
- `dim_line`: Details about each bus line
- `dim_trip`: Describes scheduled trips, including direction, pattern, etc.
- `dim_calendar_service`: Service dates, periods (e.g. school, holidays), and calendar exceptions

**Facts:**
- `fact_stop_event`: Granular fact table recording every stop event by a bus, linking all relevant dimensions
- `fact_trip_schedule`: Aggregate fact table with each scheduled trip, including duration, distance, total stops, and peak/off-peak flag

**Snapshots:**
Snapshots are used to track how dimension tables evolve over time, supporting slowly changing dimension (SCD) analysis and historical audits.

- `dim_stop_snapshot`:  
  Captures changes to stop attributes (e.g. infrastructure, accessibility, location). Useful for understanding how stop data changes across time, such as after renovations or policy updates.
    - Key columns: `stop_id`, `stop_name`, `near_school`, `dbt_scd_id`, `dbt_valid_from`, `dbt_valid_to`

- `dim_line_snapshot`:  
  Captures changes to line and route definitions, including route changes, new areas served, or branding/service modifications.
    - Key columns: `route_id`, `line_id`, `circular`, `school`

- `dim_trip_snapshot`:  
  Captures changes in scheduled trips, such as timetable updates, service type changes, or other trip-related modifications.
    - Key columns: `trip_id`, `route_id`, `service_type`, `dbt_scd_id`, `dbt_valid_from`, `dbt_valid_to`

Snapshots include automatically managed columns for SCD Type 2, allowing you to see what was valid at any point in history.

## Business Questions

This dbt project enables the analysis and answering of key business questions such as:
- How many stops exist in each municipality/region?
- Which lines serve each stop?
- How frequently does a given line stop at each location?
- Which stops are near schools?
- How many trips are scheduled per day?
- What is the average duration of a trip?
- How many trips occur during peak vs. off-peak hours?
- What is the total distance covered in each trip?
- **How did stop/line/trip definitions evolve over time?** (powered by snapshots)

> Example queries that answer these questions are available in the `/analyses` directory.

## Project Structure

models/
├── staging/
│ ├── stg_stops.sql
│ ├── stg_lines.sql
│ ├── stg_trips.sql
│ └── ...
├── marts/
│ ├── dim_stop.sql
│ ├── dim_line.sql
│ ├── dim_trip.sql
│ ├── dim_calendar_service.sql
│ ├── fact_stop_event.sql
│ └── fact_trip_schedule.sql
├── snapshots/
│ ├── dim_stop_snapshot.sql
│ ├── dim_line_snapshot.sql
│ └── dim_trip_snapshot.sql
└── analyses/
└── avgtime_trip.sql
└── daily_line_frequency.sql
└── distance_trip.sql
└── line1111_frequency.sql
└── peak_schedule.sql
└── stops_by_municipality.sql
└── stops_by_region.sql
└── stops_near_school.sql
└── top10_lines_for_stop.sql
└── trips_by_date.sql
└── which_lines_for_each_stop.sql

## How to Run

> **Prerequisites:** [dbt-core](https://docs.getdbt.com/docs/introduction), BigQuery connection configured.

1. Install dependencies:
    ```bash
    pip install dbt-bigquery
    ```
2. Configure your dbt `profiles.yml` for BigQuery authentication.
3. Run all models:
    ```bash
    dbt run
    ```
4. (Optional) Run tests:
    ```bash
    dbt test
    ```
5. Explore documentation and sample queries:
    ```bash
    dbt docs generate
    dbt docs serve
    ```

## Documentation & Comments

- All models contain descriptive comments for columns and business logic.
- Data model diagrams are available in the main project documentation.
- For questions about modeling or business logic, refer to the main project README.

---