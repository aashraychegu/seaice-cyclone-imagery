#!/usr/bin/env python3
import argparse
import subprocess
import pyfiglet
import sys
import json

def bigtext(string):
    print(pyfiglet.figlet_format(string, font="slant", width=160))

# ==============================================================================
# ARGUMENT PARSING
# ==============================================================================

parser = argparse.ArgumentParser(description="Find overlapping satellite images from matches.")

parser.add_argument("--matches", required=True, help="Path to matches Parquet file (from previous step, must contain id and geometry)")
parser.add_argument("--output", required=True, help="Output Parquet file with image overlaps")

parser.add_argument("--id-column", default="id", help="Name of ID column in matches file (default: id)")
parser.add_argument("--geometry-column", default="geometry", help="Name of geometry column in matches file (default: geometry)")
parser.add_argument("--threads", type=int, default=4, help="DuckDB threads (default: 4)")
parser.add_argument("--memory-limit", type=str, default="4GB", help="DuckDB memory limit (default: 4GB)")
parser.add_argument("--verbose", action="store_true", help="Print SQL script")

args = parser.parse_args()

# ==============================================================================
# SQL SCRIPT (OPTIMIZED)
# ==============================================================================

sql_script = f"""
INSTALL spatial;
LOAD spatial;

SET threads TO {args.threads};
SET memory_limit = '{args.memory_limit}';
SET preserve_insertion_order = false;

-- Enable DuckDB's built-in progress bar
PRAGMA enable_progress_bar;

-- Load and deduplicate in one step
CREATE OR REPLACE TABLE unique_images AS
SELECT DISTINCT 
    {args.id_column} as image_id,
    {args.geometry_column} as geometry
FROM read_parquet('{args.matches}');

-- Create spatial index
CREATE INDEX idx_geom ON unique_images USING RTREE (geometry);

-- Progress checkpoint
SELECT 'Loaded ' || COUNT(*)::VARCHAR || ' unique images' as progress FROM unique_images;

-- Find overlaps with optimized join (spatial index will be used)
CREATE OR REPLACE TABLE image_overlaps AS
SELECT 
    i1.image_id as image_id1,
    i2.image_id as image_id2,
    ST_Area(ST_Intersection(i1.geometry, i2.geometry)) as intersection_area,
    ST_Intersection(i1.geometry, i2.geometry) as overlap_geometry
FROM unique_images i1
JOIN unique_images i2
ON i1.image_id < i2.image_id 
   AND ST_Intersects(i1.geometry, i2.geometry);

-- Progress checkpoint
SELECT 'Found ' || COUNT(*)::VARCHAR || ' overlaps' as progress FROM image_overlaps;

-- Export results immediately
COPY image_overlaps TO '{args.output}' (FORMAT PARQUET, COMPRESSION GZIP);

-- Quick statistics (single pass)
COPY (
    SELECT 
        (SELECT COUNT(*) FROM unique_images) as total_images,
        COUNT(*) as total_overlaps,
        COUNT(DISTINCT image_id1) + COUNT(DISTINCT image_id2) as images_with_overlaps_approx,
        MIN(intersection_area) as min_intersection_area,
        MAX(intersection_area) as max_intersection_area,
        AVG(intersection_area) as avg_intersection_area
    FROM image_overlaps
) TO '/dev/stdout' (FORMAT JSON);
"""

# ==============================================================================
# EXECUTE
# ==============================================================================

bigtext("Executing")

if args.verbose:
    print("\n" + "="*80)
    print("SQL SCRIPT")
    print("="*80 + "\n")
    print(sql_script)

print("\n  Processing with DuckDB progress bar...\n")

# Run DuckDB with direct output (so progress bar shows)
result = subprocess.run(
    ['duckdb', ':memory:'],
    input=sql_script,
    text=True,
    capture_output=False,  # Let DuckDB output directly to terminal
    stdout=subprocess.PIPE,
    stderr=None  # stderr goes to terminal for progress bar
)

# Capture just stdout for JSON parsing
result_capture = subprocess.run(
    ['duckdb', ':memory:'],
    input=sql_script,
    text=True,
    capture_output=True
)

if result_capture.returncode != 0:
    bigtext("FAILED")
    print("\n" + "="*80)
    print("ERROR OUTPUT")
    print("="*80 + "\n")
    print(result_capture.stderr)
    sys.exit(1)

# ==============================================================================
# OUTPUT
# ==============================================================================

try:
    lines = [line for line in result_capture.stdout.strip().split('\n') if line.strip()]
    # Find the JSON line (last non-progress line)
    json_line = None
    for line in reversed(lines):
        if line.startswith('{') or '"total_images"' in line:
            json_line = line
            break
    
    if not json_line:
        raise ValueError("Could not find JSON output")
    
    stats = json.loads(json_line)
    
    bigtext("SUCCESS")
    
    print(f"\n  Matches File:             {args.matches}")
    print(f"  Threads:                  {args.threads} | Memory: {args.memory_limit}")
    
    print(f"\n  Total Images:             {stats['total_images']}")
    print(f"  Image Overlaps:           {stats['total_overlaps']}")
    
    if stats['total_overlaps'] > 0:
        print(f"  Intersection Area Range:  {stats['min_intersection_area']:.2f} - {stats['max_intersection_area']:.2f} (avg: {stats['avg_intersection_area']:.2f})")
    
    print(f"\n  ✓ Output: {args.output}\n")
        
except (json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
    print(f"\n  WARNING: Could not parse statistics: {e}")
    print(f"  Output written to: {args.output}\n")
    if args.verbose:
        print("\n" + "="*80)
        print("RAW OUTPUT")
        print("="*80 + "\n")
        print(result_capture.stdout)

sys.exit(0)