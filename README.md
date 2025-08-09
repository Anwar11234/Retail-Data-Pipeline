# Retail Data Pipeline Using Airflow 3

This project experiments with **Apache Airflow** using the [UCI Online Retail dataset](https://archive.ics.uci.edu/ml/datasets/online+retail). The dataset contains all transactions between **01/12/2010** and **09/12/2011** for a UK-based, registered non-store online retailer.

## Data Model

I designed a simple data model consisting of:

* **Dimensions:** `product`, `customer`
* **Fact tables:** `sales`, `returns`

The ETL process uses a **staging table** to temporarily hold incoming data before populating the facts and dimensions.

## Pipeline Overview

The DAG begins by creating the necessary tables (facts, dimensions, and staging).
It then:

1. **Extracts** the source data.
2. **Loads** it into the staging table.
3. **Populates** dimensions and facts using SQL queries (available in the `dags/sql` folder).

### Key Implementation Details

* **Idempotent dimension loads:** Performed using **upserts** to avoid duplicates.
* **Incremental fact table loads:** Achieved by adding a `last_updated` column and only loading rows with dates after the most recent update.

## Two Pipeline Variants

### `retail_pipeline_v3`

Processes the **entire dataset** in a single run and loads it into the warehouse.

### `retail_pipeline_v4`

Simulates **daily data ingestion** via Excel sheets:

* Each DAG run processes the file for its **execution date**.
* If no file exists for that date, **all tasks are skipped**.
* This version was backfilled from **01/12/2010** to **09/12/2011** (the dataset’s min and max dates) to load all historical data.

<p align="center">
  <img width="933" height="311" alt="DAG Graph" src="https://github.com/user-attachments/assets/2fb07482-5373-4209-a24a-a7961c263d2d" />
</p>

