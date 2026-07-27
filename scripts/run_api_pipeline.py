"""Run the provider API source flow."""

from __future__ import annotations

import sys

import run_pipeline

if __name__ == "__main__":
    sys.argv = [sys.argv[0], "--mode", "api", *sys.argv[1:]]
    raise SystemExit(run_pipeline.main())
