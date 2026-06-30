'''neon_plot_writer.py'''

import os
import psycopg2

def neon_upsert_plot(wy_id: int, plot_type: str, png_bytes: bytes) -> None:
    """
    Upserts a PNG blob into plots.blobs.
    Assumes the schema/table already exist:

        CREATE SCHEMA IF NOT EXISTS plots;
        CREATE TABLE IF NOT EXISTS plots.blobs (
            wy_id        INTEGER,
            plot_type    TEXT,
            png_data     BYTEA,
            generated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY  (wy_id, plot_type)
        );
    """
    conn = psycopg2.connect(os.environ['NEON_DSN'])
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO plots.blobs (wy_id, plot_type, png_data)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (wy_id, plot_type)
                    DO UPDATE SET
                        png_data     = EXCLUDED.png_data,
                        generated_at = now()
                """, (wy_id, plot_type, psycopg2.Binary(png_bytes)))
    finally:
        conn.close()