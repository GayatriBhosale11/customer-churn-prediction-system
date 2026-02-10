# Customer Churn Prediction System

An end-to-end machine learning project for customer churn prediction. The project covers data ingestion, validation, preprocessing, feature engineering, feature storage, model training, DVC-based versioning, and Airflow orchestration.

## What This Project Does

- Downloads customer churn data from a CSV source and a Hugging Face dataset endpoint
- Validates the latest raw files and generates a data quality report
- Cleans and prepares the data for modeling
- Builds transformed training datasets and stores feature metadata
- Populates a lightweight feature store
- Trains a churn prediction model with MLflow logging
- Provides both local pipeline execution and Airflow-based orchestration

## Tech Stack

- Python
- Pandas, NumPy, scikit-learn
- MLflow
- SQLite
- DVC
- Apache Airflow
- Matplotlib, Seaborn

## Simplified Project Structure

```text
Customer Churn Prediction System/
├── airflow/
│   ├── dags/
│   │   └── churn_prediction_pipeline.py
│   └── setup_airflow.py
├── config/
│   └── dvc/
├── data/
│   ├── eda/
│   ├── feature_store/
│   └── models/
├── database/
│   └── init.sql
├── docs/
├── logs/
├── src/
│   ├── build_model.py
│   ├── data_ingestion.py
│   ├── data_preparation.py
│   ├── data_transformation_storage.py
│   ├── data_validation.py
│   ├── data_versioning.py
│   ├── feature_store.py
│   ├── pipeline_tasks.py
│   ├── raw_data_storage.py
│   └── utils/
│       ├── logger.py
│       └── path_helpers.py
├── docker-compose.yml
├── Dockerfile
├── main_pipeline.py
└── requirements.txt
```

## Pipeline Flow

1. Data ingestion
2. Raw data cataloging
3. Data validation
4. Data preparation
5. Data transformation
6. Feature store update
7. Data version tagging
8. Model training

## Setup

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip3 install -r requirements.txt
```

### 3. Run the full pipeline

```bash
python3 main_pipeline.py
```

## Run Individual Pipeline Steps

```bash
python3 src/pipeline_tasks.py --step data_ingestion
python3 src/pipeline_tasks.py --step data_validation
python3 src/pipeline_tasks.py --step data_preparation
python3 src/pipeline_tasks.py --step data_transformation
python3 src/pipeline_tasks.py --step feature_store
python3 src/pipeline_tasks.py --step model_building
```

To run everything through the shared task runner:

```bash
python3 src/pipeline_tasks.py --step full
```

## Main Outputs

- Raw datasets in `data/raw/`
- Validation reports in `reports/`
- Cleaned and transformed datasets in `data/processed/`
- Feature store files in `data/feature_store/`
- Trained models in `data/models/`
- Logs in `logs/`

## Airflow

The DAG file is available at `airflow/dags/churn_prediction_pipeline.py`.

It now uses the shared pipeline task runner so local execution and Airflow execution follow the same step definitions.

## Notes

- The project is designed for learning and demonstration of an end-to-end ML pipeline.
- DVC support is included for data versioning workflows.
- MLflow is used for experiment tracking during model training.

## Author

**GAYATRI BHOSALE**  
GitHub: [https://github.com/GayatriBhosale11](https://github.com/GayatriBhosale11)
Updated on Feb 10
