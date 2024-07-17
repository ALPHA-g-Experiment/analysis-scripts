# Analysis-Scripts

[![Test Status](https://github.com/ALPHA-g-Experiment/analysis-scripts/actions/workflows/python.yml/badge.svg)](https://github.com/ALPHA-g-Experiment/analysis-scripts/actions/workflows/python.yml)
![Supported Versions](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue?labelColor=383f47)

The `bin/` directory contains scripts to analyze the CSV files produced by the
core binaries of the
[`alpha-g-analysis`](https://github.com/ALPHA-g-Experiment/alpha-g/tree/main/analysis)
package.

## Getting Started

Clone the repository, setup a virtual environment, and install the required
dependencies:

```bash
git clone https://github.com/ALPHA-g-Experiment/analysis-scripts.git
cd analysis-scripts
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

Run each script with the `--help` flag to see the available options, e.g.:

```bash
cd bin
python3 vertices.py --help
```
