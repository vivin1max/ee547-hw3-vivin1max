-- Drop existing tables for idempotent runs
DROP TABLE IF EXISTS stop_events CASCADE;
DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS line_stops CASCADE;
DROP TABLE IF EXISTS stops CASCADE;
DROP TABLE IF EXISTS lines CASCADE;

-- Lines: transit routes
CREATE TABLE lines (
    line_id SERIAL PRIMARY KEY,
    line_name VARCHAR(50) NOT NULL UNIQUE,
    vehicle_type VARCHAR(10) NOT NULL,
    CONSTRAINT vehicle_type_chk CHECK (vehicle_type IN ('rail', 'bus'))
);

-- Stops: stop locations
CREATE TABLE stops (
    stop_id SERIAL PRIMARY KEY,
    stop_name VARCHAR(120) NOT NULL UNIQUE,
    latitude NUMERIC(9,6) NOT NULL,
    longitude NUMERIC(9,6) NOT NULL,
    CONSTRAINT lat_range_chk CHECK (latitude >= -90 AND latitude <= 90),
    CONSTRAINT lon_range_chk CHECK (longitude >= -180 AND longitude <= 180)
);

-- Line stops: mapping of stops to a line in order
CREATE TABLE line_stops (
    line_id INTEGER NOT NULL REFERENCES lines(line_id) ON DELETE CASCADE,
    stop_id INTEGER NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL,
    time_offset_minutes INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (line_id, sequence),
    CONSTRAINT seq_positive_chk CHECK (sequence >= 1),
    CONSTRAINT time_offset_nonneg_chk CHECK (time_offset_minutes >= 0)
);

-- Trips: scheduled vehicle runs on a line
CREATE TABLE trips (
    trip_id VARCHAR(32) PRIMARY KEY,
    line_id INTEGER NOT NULL REFERENCES lines(line_id) ON DELETE RESTRICT,
    scheduled_departure TIMESTAMP NOT NULL,
    vehicle_id VARCHAR(32) NOT NULL
);

-- Stop events: actual arrivals/departures during trips
CREATE TABLE stop_events (
    trip_id VARCHAR(32) NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
    stop_id INTEGER NOT NULL REFERENCES stops(stop_id) ON DELETE RESTRICT,
    scheduled TIMESTAMP NOT NULL,
    actual TIMESTAMP NOT NULL,
    passengers_on INTEGER NOT NULL DEFAULT 0,
    passengers_off INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (trip_id, stop_id),
    CONSTRAINT pax_nonneg_on CHECK (passengers_on >= 0),
    CONSTRAINT pax_nonneg_off CHECK (passengers_off >= 0)
);

-- Helpful indexes for query performance
CREATE INDEX IF NOT EXISTS idx_line_stops_line_seq ON line_stops(line_id, sequence);
CREATE INDEX IF NOT EXISTS idx_stop_events_trip ON stop_events(trip_id);
CREATE INDEX IF NOT EXISTS idx_stop_events_stop ON stop_events(stop_id);
CREATE INDEX IF NOT EXISTS idx_trips_departure ON trips(scheduled_departure);
