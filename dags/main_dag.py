"""Compatibility note for the Airflow entry points.

The pipeline now has two explicit source-specific DAGs:

- json_source_dag.py -> eat_ngo_json_source_pipeline
- api_source_dag.py  -> eat_ngo_api_source_pipeline

This file intentionally does not register a DAG. Keeping only the two source
flows active avoids duplicate schedules writing to the same warehouse file.
"""
