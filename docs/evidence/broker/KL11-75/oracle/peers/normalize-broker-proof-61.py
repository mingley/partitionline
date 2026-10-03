#!/usr/bin/env python3
"""Run the additive frozen51qualification+10maintenance normalizer."""
from pathlib import Path
import runpy


if __name__ == "__main__":
    runpy.run_path(str(Path(__file__).parent / "normalization-61-preparation"
                      / "normalize-broker-proof-51-maintenance.py"), run_name="__main__")
