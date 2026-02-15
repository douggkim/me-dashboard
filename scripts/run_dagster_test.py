import argparse
import subprocess
import sys


def run_dagster_materialize(asset_name: str, partition_date: str):
    """
    Runs `dagster asset materialize` for a given asset and partition date.

    Args:
        asset_name: The name of the asset to materialize.
        partition_date: The partition date to use.
    """
    command = [
        "dagster",
        "asset",
        "materialize",
        "-f",
        "src/main.py",
        "-a",
        "defs",
        "--select",
        asset_name,
        "--partition",
        partition_date,
    ]

    print(f"Running command: {' '.join(command)}")

    try:
        # Using capture_output=True to get stdout and stderr
        # Using text=True to get stdout and stderr as strings
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            # Assumes dagster commands are run from the root of the project
            # and the `src` module is in the python path.
            # The `-m src` is a common pattern for dagster projects.
            # We are assuming it's not needed if we are in the project root
            # and the virtual environment is activated.
        )
        print("Dagster materialization successful:")
        print("--- stdout ---")
        print(result.stdout)
        print("--- stderr ---")
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error during Dagster materialization for asset {asset_name}:", file=sys.stderr)
        print(e.stdout, file=sys.stdout)
        print(e.stderr, file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print("Error: 'dagster' command not found. Make sure you are in the correct environment.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a Dagster asset materialization test.")
    parser.add_argument("asset_name", type=str, help="The name of the asset to materialize.")
    parser.add_argument("partition_date", type=str, help="The partition date in YYYY-MM-DD format.")

    args = parser.parse_args()

    run_dagster_materialize(args.asset_name, args.partition_date)
