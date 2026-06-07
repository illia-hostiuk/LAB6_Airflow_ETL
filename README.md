# LAB6_Airflow_ETL

This project demonstrates an hourly ETL pipeline using Apache Airflow and Astro.

## ETL steps

- Extract: generates sample nested JSON data.
- Transform: reduces JSON nesting by flattening fields.
- Load: creates a pandas DataFrame and prints it to the Airflow logs.

## Run project

```bash
astro dev start
