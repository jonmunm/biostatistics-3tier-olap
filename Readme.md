# 🚀 Project: Biostatistics OLAP (3-Tier Architecture)

Analytical and Big Data processing system developed with a 3-tier OLAP architecture (**Raw, Silver, Gold**), designed to process, clean, and structure historical hospital discharge records and public health statistics in Chile.

---

## 🛠️ Technology Stack

The project integrates modern data engineering, containerization, and orchestration tools:

*   **Orchestration:** Apache Airflow 3.0.0 (featuring TaskGroups, custom operators, and short-circuits evaluating Jinja templates).
*   **Distributed Processing:** Apache PySpark 3.4.0 (Master-Worker architecture with Spark Submit).
*   **Analytical Storage (OLAP):** DuckDB (High-performance embedded analytical database engine).
*   **Data Quality:** Great Expectations (validating schemas, ranges, types, and nulls across layers).
*   **Containerization & Infrastructure:** Docker and Docker Compose (modularized under the `infra/` folder).
*   **Language:** Python 3.10 / 3.12.

---

## 📂 Project Structure

```text
biostatistics-3tier-olap/
├── infra/                  # Infrastructure and Docker Compose files
│   ├── .env                # Project environment variables
│   ├── docker-compose.yaml
│   ├── *.compose.yaml      # Modular configuration per service (Airflow, Spark, DB)
│   └── *.Dockerfile        # Custom images for Airflow, Spark, and Jupyter
├── src/                    # Application source code
│   ├── airflow/            # Airflow DAGs, plugins, and configurations
│   │   ├── dags/           # Pipeline definitions (load_discharges, drop_discharges)
│   │   └── groups/         # Modular task groups (init, compute, ingestion)
│   ├── spark/              # PySpark ETL scripts (Raw, Silver, Gold layers & utilities)
│   ├── sql_schema/         # SQL initialization and cleanup scripts for DuckDB
│   └── tests/              # Data quality tests using Great Expectations
├── data/                   # Local data directory and DuckDB storage
```

## 🏗️ 3-Tier Architecture (Medallion Architecture)

* **Raw Layer:** Ingestion of heterogeneous hospital discharge CSV files (multiple historical structures grouped by year) from cloud storage, unifying them into raw Parquet format.

* **Silver Layer:** Standardization, text cleaning, null imputation, year extraction using regular expression parsing, and strict analytical column typing.

* **Gold Layer:** Dimensional enrichment via optimized broadcast joins, recoding causes of death, and territorial CUT code validation to populate the analytical fact table.

## 📖 Data Dictionary (Main Dimensions)
Below is the schema breakdown of the master dimensions supporting the Gold layer star schema:

### 1. ```dim_gender``` (Gender)

* ```id```: Unique identifier for gender.
* ```gender_name```: Textual description of gender (e.g., Masculino, Femenino).
* ```gender_shortname```: Abbreviation or initial.

### 2. ```dim_age``` (Age and Age Groups)

* ```id / age_id```: Unique identifier for age.
* ```age_name```: Description or age range.
* ```fiveyear_span_name / tenyear_span_name```: Five-year and ten-year groupings for demographic analysis.

### 3. ```dim_year``` (Year)

* ```id```: Sequential identifier for the year.
* ```real_year```: Actual calendar year (e.g., 2001, 2018).

### 4. ```dim_health_insurance``` (Health Insurance / Previsión)

* ```id```: Unique record identifier.
* ```health_insurance_code```: Original insurance code reported in discharges.
* ```health_insurance_name```: Name of the insurance provider or scheme (e.g., Fonasa, Isapre, Ley 16.744).
* ```valid_from / valid_to``` (SCD Type 2): Historical validity year range for dimensional mapping.

### 5. ```dim_healthcare_service``` (Healthcare Service)

* ```id```: Unique identifier for the healthcare service.
* ```healthcare_service_cut```: CUT code of the healthcare service.
* ```healthcare_service_name```: Official name of the healthcare service.
* ```region_name```: Associated region.

### 6. ```dim_city``` (Communes and Territory)

* ```id```: Unique identifier for the commune.
* ```city_cut```: Unique Territorial Code (CUT) of the commune.
* ```city_name```: Name of the commune.
* ```province_name & region_name```: Higher administrative hierarchy.

### 7. ```dim_cause_of_death``` (Causes of Death - ICD-10)

* ```id```: Unique identifier for the cause of death subcategory.
* ```subcategory_code```: ICD-10 code (e.g., A000).
* ```subcategory_name```: Clinical description of the cause.
* ```chapter_name```: General chapter of the morbidity/mortality classification.

### 8. ```fact_biostatistics``` (Causes of Death - ICD-10)

The ```fact_biostatistics``` table is implemented as a Factless Fact Table within a Star Schema architecture. Unlike traditional transactional fact tables that store numeric measurements or additive metrics (such as revenue, quantities, or durations), this table captures the co-occurrence, intersection, and relationship among multiple descriptive dimensions for each hospital discharge event.

#### Key Architectural Characteristics:
* **Intersection of Dimensions (Event Mapping):** Each row represents a single hospital discharge event, defined entirely by the convergence of its contextual attributes—such as the patient's demographic profile (age, gender), the administrative and territorial context (city, healthcare service), the temporal reference (year), the coverage scheme (health insurance), and the clinical diagnoses (main and external causes of death).

* **Absence of Additive Metrics:** Since the primary objective is to analyze categorical distributions, frequencies, and multi-dimensional cross-tabulations (e.g., mortality rates grouped by age, region, and diagnosis), storing numeric metrics is unnecessary. Analysis is performed by counting the number of occurrences (COUNT(*)) or distinct events mapped to specific dimensional attributes.

* **Referential Integrity & Star Schema Optimization:** By leveraging foreign keys (_id) pointing to master dimension tables, the table achieves high storage efficiency and enables lightning-fast analytical queries and OLAP cubes aggregation using DuckDB.