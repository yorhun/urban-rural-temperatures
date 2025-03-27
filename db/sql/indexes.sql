-- Index on locations table for faster location lookups
-- Not exactly necessary for few locations but keeping in case the table grows in the future.
CREATE INDEX idx_locations_name ON locations(name);
CREATE INDEX idx_locations_urban_pair ON locations(urban_pair_id);

-- Create an index on the timestamp column for improved query performance
CREATE INDEX idx_temperature_timestamp 
ON temperature_data(timestamp);