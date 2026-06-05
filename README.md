# 🏃‍♂️ Ultramarathon Data Engineering Platform (Databricks)

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=Apache%20Spark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00A9E0?style=for-the-badge&logo=Databricks&logoColor=white)

This repository contains an end-to-end Data Engineering pipeline built in **Databricks** as part of a Cloud and Big Data lab at STI. The project processes and models historical ultramarathon data to enable advanced analytics and AI/BI querying.

## 🎥 Video Presentation
*(Click the image below to watch the project presentation)*

<a href="https://www.youtube.com/watch?v=MG5afHCvnNc">
  <img src="./img/thumbnail_yt.png" alt="Video presentation" width="600">
</a>

---

## Dataset Overview
The primary dataset used is the **Two Centuries of UM Races** from [Kaggle](https://www.kaggle.com/datasets/sudalairajkumar/two-centuries-of-um-races) combined with country code mappings. It contains over 7.4 million rows representing individual race performances from 1798 to 2022.

---

## Architecture (Medallion Pipeline)
The pipeline is structured using the Medallion Architecture (Bronze, Silver, Gold) utilizing **Delta Lake** and **Unity Catalog**. 

### Bronze Layer (Raw Data Ingestion)
- Uses **PySpark Structured Streaming** (`Auto Loader`) to ingest raw CSV files (`TWO_CENTURIES_OF_UM_RACES.csv` and `code_countries.csv`) into Delta tables.
- Schema inference and raw data preservation.

### Silver Layer (Data Cleansing & OBT)
- **Data Cleansing:** Standardizes date formats, cleans event names, and calculates consistent speeds, distances, and durations.
- **Data Quality & Outlier Handling:** Based on exploratory data analysis (EDA) and real-world limits:
  - Excludes runners under 15 or over 100 years old (based on [Fauja Singh's record](https://www.olympics.com/en/news/who-is-fauja-singh-oldest-indian-origin-british-marathon-runner)).
  - Filters out impossible speeds > 21.19 km/h (based on the [Marathon World Record](https://en.wikipedia.org/wiki/Marathon_world_record_progression)).
- **Identity Resolution:** Solves historical ID conflicts by generating guaranteed unique IDs using `SHA-256` hashing on a combination of ID, gender, country, and birth year.
- Outputs a clean **One Big Table (OBT)** (`marathon_obt`).

### Gold Layer (Dimensional Modeling & Data Marts)
Transforms the Silver OBT into a **Star Schema** optimized for BI and downstream analytics using Spark SQL.
- **Fact Table:** `fct_results`
- **Dimension Tables:** `dim_athlete`, `dim_event`, `dim_date`
- **Data Marts:** Created materialized views (`mart_distance`, `mart_time`) merging facts and dimensions tailored for specific dashboard needs and conversational AI querying.

---

## 🤖 AI/BI Integration (Databricks Genie)
The Gold Data Marts are highly optimized to be used with **Databricks Genie**. The pipeline ensures that columns are clearly named and appropriately typed, allowing users to ask natural language questions like:
* *"What is the distribution of events by event country?"*
* *"How much faster are men than women at running 50 km year by year?"*

---

## 🔗 References & Research
- [Northstowe Half Marathon 2027 - Age Groups](https://northstowehalf.co.uk/age-groups/)
- [Wikipedia - Marathon world record progression](https://en.wikipedia.org/wiki/Marathon_world_record_progression)
- [Olympics.com - Fauja Singh (Oldest Marathon Runner)](https://www.olympics.com/en/news/who-is-fauja-singh-oldest-indian-origin-british-marathon-runner)