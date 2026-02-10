#!/usr/bin/env python3
"""Thin entrypoint for running the full churn pipeline locally."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from pipeline_tasks import run_full_pipeline


if __name__ == "__main__":
    raise SystemExit(0 if run_full_pipeline() else 1)
