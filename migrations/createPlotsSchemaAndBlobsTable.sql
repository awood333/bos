CREATE SCHEMA IF NOT EXISTS plots;
CREATE TABLE IF NOT EXISTS plots.blobs (
    wy_id        INTEGER,
    plot_type    TEXT,
    png_data     BYTEA,
    generated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY  (wy_id, plot_type)
);