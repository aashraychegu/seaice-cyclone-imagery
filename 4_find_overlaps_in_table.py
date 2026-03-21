#!/usr/bin/env python3
import argparse
import duckdb


def main():
    p = argparse.ArgumentParser(
        description="Compute before/after intersecting geometry pairs per point_id from {}}_matches."
    )
    p.add_argument("--db", required=True, help="DuckDB database file used by the matching script.")
    p.add_argument(
        "--matches-table",
        required = True,
        help="Matches table name (default: input_points_matches).",
    )
    p.add_argument(
        "--output-table",
        default=None,
        help="Output table name (default: {matches_table}_overlaps).",
    )
    p.add_argument("--threads", type=int, default=32)
    p.add_argument("--memory-limit", default="16GB")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    out_table = args.output_table or f"{args.matches_table}_overlaps"

    con = duckdb.connect(args.db)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads TO {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")

    sql = f"""
    CREATE OR REPLACE TABLE {out_table} AS
    WITH deduplicated AS (
      SELECT DISTINCT ON (geometry)
        point_id, geometry, id, datetime_start, point_datetime
      FROM {args.matches_table}
    )
    SELECT
      a.point_id,
      a.geometry as geometry_a,
      b.geometry as geometry_b,
      (ST_Area(ST_Intersection(a.geometry, b.geometry)) / ST_Area(a.geometry) * 100) as pct_a,
      (ST_Area(ST_Intersection(a.geometry, b.geometry)) / ST_Area(b.geometry) * 100) as pct_b,
      a.id as id_a,
      b.id as id_b,
      a.datetime_start as datetime_start_a,
      b.datetime_start as datetime_start_b,
      a.point_datetime as point_datetime
    FROM deduplicated a
    JOIN deduplicated b
      ON a.point_id = b.point_id
     AND ST_Intersects(a.geometry, b.geometry)
     AND a.id < b.id
     AND a.datetime_start < a.point_datetime
     AND b.datetime_start > a.point_datetime
     AND a.datetime_start < b.datetime_start;
    """

    if args.verbose:
        print(sql)

    con.execute(sql)

    # Small summary to stdout
    n = con.execute(f"SUMMARIZE SELECT * FROM {out_table}").fetchdf()
    mid = (len(n.columns) + 1) // 2
    left = n.iloc[:, :mid]
    right = n.iloc[:, mid:]
    print(f"Created table: {out_table}")
    print(left)
    print(right)


if __name__ == "__main__":
    main()