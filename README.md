# Lethe
Lethe is a local data engineering project for ingesting, cleaning, and anonymising
sensitive transactional data using DuckDB and Python.

## Set Up

### Requirements
- Python 3.11
- PDM

### Installation
Clone the repository and install the project dependencies:

```bash
pdm install
```

Your raw data files should be placed on `lethe/data/raw`. The duckdb config file will be created on `lethe/duckdb`.