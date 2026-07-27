"""Run the MinIO-backed JSON source flow."""

from __future__ import annotations

import sys

import run_pipeline

if __name__ == "__main__":
    sys.argv = [sys.argv[0], "--mode", "json", *sys.argv[1:]]
    raise SystemExit(run_pipeline.main())
