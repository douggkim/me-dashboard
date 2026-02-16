---
name: data_analysis
description: Perform data discovery and execute analytical queries on project data using DuckDB.
---

# Data Analysis Skill

This skill allows you to perform data analysis on the project's data, including local files and S3-backed Delta tables, using DuckDB.

## Usage

To use this skill, you will create Python scripts that utilize the `duckdb` library and then execute them using `uv run`.

### Setup

You do not need to install `duckdb` manually; it should be available in the environment or installed via `uv` if specified in a script's dependencies (though for this project, rely on the environment's `dagster-duckdb` which provides `duckdb`).

### Quering Data

Create a python script (e.g., `analyze_data.py`) with the following template to configure S3 access and query data.

**Template:**

```python
import duckdb

def main():
    con = duckdb.connect()
    
    # Install and load httpfs extension for S3/MinIO access
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    
    # Configure S3/MinIO credentials
    con.execute("SET s3_endpoint = 'minio:9000';")
    con.execute("SET s3_access_key_id = 'rTFidVYcxQjiYCZOXhm8';")
    con.execute("SET s3_secret_access_key = 'BOEhYByB6e645RKiVTt2kvlMR2qCz2jVe90IfD5F';")
    con.execute("SET s3_use_ssl = false;")
    con.execute("SET s3_url_style = 'path';")

    # Example Query: Read from a Delta Table in MinIO
    # Note: Use 's3://' scheme.
    # query = "SELECT * FROM delta_scan('s3://me-dashboard/silver/location/location_data_silver') LIMIT 5;"
    
    # Example Query: Read from a local Parquet/CSV file
    # query = "SELECT * FROM 'data/testing/bronze/some_file.csv' LIMIT 5;"

    # Write your specific query here
    query = "SELECT * FROM delta_scan('s3://me-dashboard/silver/location/location_data_silver') LIMIT 5;"
    
    results = con.execute(query).fetchall()
    print(results)
    
    # To get a pandas DataFrame (if pandas is available)
    # df = con.execute(query).df()
    # print(df)

if __name__ == "__main__":
    main()
```

### Execution

Run the script using `uv run`:

```bash
uv run python analyze_data.py
```

### Discovery

Before querying, use `find` to discover available data files:

```bash
find data/ -name "*.parquet" -o -name "*.csv" -o -name "*.json" | head -20
```

## Troubleshooting

- **S3 Connection Issues**: Ensure the MinIO service is running and accessible. The credentials in the template are specific to the local dev environment.
- **Missing Extensions**: If `INSTALL httpfs;` fails, ensure internet access is allowed or the extension is already pre-loaded.
