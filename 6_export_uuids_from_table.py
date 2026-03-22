#!/usr/bin/env python3
import argparse
import duckdb


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Export UUIDs for download from an overlaps table'
    )
    parser.add_argument(
        '--db',
        required=True,
        help='Path to the DuckDB database file containing the overlaps and source tables.',
    )
    parser.add_argument(
        '--overlaps-table',
        required=True,
        help='Name of the overlaps table to process.',
    )
    parser.add_argument(
        '--output-table',
        default=None,
        help='Output table name (default: {overlaps_table}_uuids).',
    )
    parser.add_argument('--threads', type=int, default=32)
    parser.add_argument('--memory-limit', default='16GB')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    out_table = args.output_table or f"{args.overlaps_table}_uuids"

    con = duckdb.connect(args.db)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads TO {args.threads}")
    con.execute(f"SET memory_limit = '{args.memory_limit}'")

    # First, get the source_table value (assuming all rows have the same source_table)
    source_table_result = con.execute(
        f"SELECT DISTINCT source_table FROM {args.overlaps_table} LIMIT 1"
    ).fetchone()
    
    if not source_table_result:
        raise ValueError(f"No source_table found in {args.overlaps_table}")
    
    source_table = source_table_result[0]

    sql = f"""
    CREATE OR REPLACE TABLE {out_table} AS
    WITH 
        -- Step 1: Unpivot the 'before' and 'after' columns into a consistent stream of records.
        unpivoted_data AS (
            SELECT 
                id_before AS id,
                datetime_start_before AS start_datetime,
                geometry_before AS geometry
            FROM {args.overlaps_table}
            WHERE id_before IS NOT NULL

            UNION ALL

            SELECT 
                id_after AS id,
                datetime_start_after AS start_datetime,
                geometry_after AS geometry
            FROM {args.overlaps_table}
            WHERE id_after IS NOT NULL
        ),

        -- Step 2: Calculate the centroid geometry for each record.
        with_centroids AS (
            SELECT
                *,
                ST_Centroid(geometry) AS centroid_geom
            FROM unpivoted_data
        )

    -- Step 3: Select the final columns, join with the source table, and format geometry outputs.
    SELECT 
        c.id,
        c.start_datetime,
        s.s3_path,
        ST_AsText(c.centroid_geom) AS centroid_wkt,
        ST_Y(c.centroid_geom) AS lat, -- Latitude (Y coordinate)
        ST_X(c.centroid_geom) AS lon  -- Longitude (X coordinate)
    FROM with_centroids c
    LEFT JOIN {source_table} s ON c.id = s.id
    ORDER BY start_datetime, c.id;
    """

    if args.verbose:
        print(f"Source table: {source_table}")
        print(sql)

    print(f"Loading overlaps table: {args.overlaps_table}")
    print(f"Processing records and creating output table...")

    con.execute(sql)

    summary = con.execute(f"SUMMARIZE SELECT * FROM {out_table}").fetchdf()
    mid = (len(summary.columns) + 1) // 2
    left = summary.iloc[:, :mid]
    right = summary.iloc[:, mid:]

    print(f"\nCreated table: {out_table}")
    print(left)
    print(right)


if __name__ == "__main__":
    main()