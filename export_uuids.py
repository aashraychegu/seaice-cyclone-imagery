import subprocess
import sys
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(
    description='Extract unique IDs and S3 paths from id_a and id_b columns in a Parquet file'
)
parser.add_argument('--input-file', type=str, help='Path to input Parquet file', required=True)
parser.add_argument('--output-file', type=str, help='Path to output CSV file', required=True)
parser.add_argument('--db-file', type=str, help='Path to DuckDB database file', required=True)

args = parser.parse_args()

# Validate input file exists
if not Path(args.input_file).exists():
    print(f"✗ Error: Input file not found: {args.input_file}", file=sys.stderr)
    sys.exit(1)

# Validate database file exists
if not Path(args.db_file).exists():
    print(f"✗ Error: Database file not found: {args.db_file}", file=sys.stderr)
    sys.exit(1)

# Build the DuckDB SQL command to get distinct UUIDs with S3 paths
duckdb_command = f"""
ATTACH '{args.db_file}' AS db;

COPY (
    SELECT DISTINCT 
        combined.id,
        s1.s3_path
    FROM (
        SELECT id_a AS id FROM read_parquet('{args.input_file}')
        UNION
        SELECT id_b AS id FROM read_parquet('{args.input_file}')
    ) combined
    LEFT JOIN db.sentinel1 s1 ON combined.id = s1.id
    ORDER BY combined.id
) TO '{args.output_file}' (HEADER, DELIMITER ',');
"""

print(f"Loading Parquet file: {args.input_file}")
print(f"Loading database: {args.db_file}")
print(f"Extracting unique IDs with S3 paths...")

# Execute DuckDB command
result = subprocess.run(
    ['duckdb', ':memory:'],
    input=duckdb_command,
    text=True,
    capture_output=True,
    check=True
)

print(f"✓ Query executed successfully")
print(f"✓ Unique IDs with S3 paths saved to: {args.output_file}")

if result.stdout:
    print(f"\nDuckDB output:\n{result.stdout}")