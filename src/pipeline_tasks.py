#!/usr/bin/env python3
"""Shared pipeline task entrypoints for local runs and Airflow."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Callable

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.append(str(CURRENT_DIR))

def run_data_ingestion_step() -> dict[str, Any]:
    """Fetch source data into the raw data directory."""
    from data_ingestion import DataIngestionPipeline

    print("Step 2: Running data ingestion...")
    pipeline = DataIngestionPipeline()
    return pipeline.run_ingestion()


def run_raw_data_storage_step() -> str:
    """Create a catalog for the currently available raw data."""
    from raw_data_storage import RawDataStorage

    print("Step 3: Creating raw data catalog...")
    storage = RawDataStorage()
    return storage.create_data_catalog()


def run_data_validation_step() -> dict[str, Any]:
    """Validate the latest raw data files."""
    from data_validation import DataValidator

    print("Step 4: Running data validation...")
    validator = DataValidator()
    return validator.run_validation()


def run_data_preparation_step() -> Any:
    """Clean, encode, and scale the latest raw CSV."""
    from data_preparation import DataPreparationPipeline

    print("Step 5: Running data preparation...")
    preparation = DataPreparationPipeline()
    return preparation.run_preparation_auto()


def run_data_transformation_step() -> tuple[Any, str]:
    """Create transformed features and a training set."""
    from data_transformation_storage import DataTransformationStorage

    print("Step 6: Running data transformation...")
    transformation = DataTransformationStorage()
    try:
        return transformation.run_transformation_pipeline_auto()
    finally:
        transformation.close_connection()


def run_feature_store_step() -> dict[str, str]:
    """Populate the feature store from the latest processed dataset."""
    from feature_store import SimpleChurnFeatureStore

    print("Step 7: Updating feature store...")
    feature_store = SimpleChurnFeatureStore()
    try:
        message = feature_store.auto_populate_from_latest_data()
        return {"message": message, "store_path": feature_store.store_path}
    finally:
        feature_store.close()


def run_data_versioning_step() -> str:
    """Create a version tag for an end-to-end pipeline run."""
    from data_versioning import version_pipeline_step

    print("Step 8: Recording pipeline version...")
    return version_pipeline_step(
        "Airflow Pipeline Complete",
        "Complete pipeline run from shared task runner"
    )


def run_model_training_step() -> dict[str, str]:
    """Train the default churn model."""
    from build_model import TrainCustomModel

    print("Step 9: Running model training...")
    model_builder = TrainCustomModel()
    model_builder.train_model(model_type="logistic_regression")
    return {"status": "completed", "model_dir": "data/models"}


STEP_RUNNERS: dict[str, Callable[[], Any]] = {
    "data_ingestion": run_data_ingestion_step,
    "raw_data_storage": run_raw_data_storage_step,
    "data_validation": run_data_validation_step,
    "data_preparation": run_data_preparation_step,
    "data_transformation": run_data_transformation_step,
    "feature_store": run_feature_store_step,
    "data_versioning": run_data_versioning_step,
    "model_building": run_model_training_step,
}


def print_pipeline_results(
    ingestion_result: dict[str, Any],
    storage_result: str,
    validation_result: dict[str, Any],
    preparation_result: Any,
    transformation_result: tuple[Any, str],
    feature_store_result: dict[str, str],
    raw_version_tag: str,
    cleaned_version_tag: str,
    transformed_version_tag: str,
    final_version_tag: str,
    model_result: dict[str, str],
) -> None:
    """Print a compact summary for local runs."""
    transformed_df, training_path = transformation_result

    print("\nPipeline completed successfully!")
    print(f"CSV File: {ingestion_result['csv_file']}")
    print(f"Hugging Face File: {ingestion_result['huggingface_file']}")
    print(f"Data Catalog: {storage_result}")
    print(f"Validation Report: {validation_result['report_path']}")
    print(f"Prepared Shape: {preparation_result.shape}")
    print(f"Training Set: {training_path}")
    print(f"Transformed Shape: {transformed_df.shape}")
    print(f"Feature Store: {feature_store_result['message']}")
    print(f"Feature Store Path: {feature_store_result['store_path']}")
    print(f"Raw Data Version: {raw_version_tag}")
    print(f"Cleaned Data Version: {cleaned_version_tag}")
    print(f"Transformed Data Version: {transformed_version_tag}")
    print(f"Final Version: {final_version_tag}")
    print(f"Model Output: {model_result['model_dir']}")
    print("Check logs: logs/")


def run_full_pipeline() -> bool:
    """Run the complete local pipeline."""
    from data_versioning import version_pipeline_step

    print("Customer Churn Data Management Pipeline")
    print("=" * 50)

    try:
        ingestion_result = run_data_ingestion_step()
        raw_version_tag = version_pipeline_step(
            "Data Ingestion",
            "Raw data from ingestion pipeline"
        )
        storage_result = run_raw_data_storage_step()
        validation_result = run_data_validation_step()
        preparation_result = run_data_preparation_step()
        cleaned_version_tag = version_pipeline_step(
            "Data Preparation",
            "Cleaned and preprocessed data"
        )
        transformation_result = run_data_transformation_step()
        transformed_version_tag = version_pipeline_step(
            "Data Transformation",
            "Transformed features for ML training"
        )
        feature_store_result = run_feature_store_step()
        final_version_tag = version_pipeline_step(
            "Pipeline Complete",
            "Complete pipeline with all processed data"
        )
        model_result = run_model_training_step()

        print_pipeline_results(
            ingestion_result,
            storage_result,
            validation_result,
            preparation_result,
            transformation_result,
            feature_store_result,
            raw_version_tag,
            cleaned_version_tag,
            transformed_version_tag,
            final_version_tag,
            model_result,
        )
        return True

    except ImportError as exc:
        print(f"Import error: {exc}")
        print("Make sure all required modules are installed and accessible.")
    except FileNotFoundError as exc:
        print(f"File not found: {exc}")
        print("Make sure all required data files and directories exist.")
    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        print("Check the logs for more detailed error information.")

    return False


def main() -> int:
    """CLI for shared pipeline steps."""
    parser = argparse.ArgumentParser(description="Run churn pipeline tasks.")
    parser.add_argument(
        "--step",
        choices=["full", *STEP_RUNNERS.keys()],
        default="full",
        help="Run the full pipeline or a single registered step.",
    )
    args = parser.parse_args()

    if args.step == "full":
        return 0 if run_full_pipeline() else 1

    result = STEP_RUNNERS[args.step]()
    print(f"{args.step} result: {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
