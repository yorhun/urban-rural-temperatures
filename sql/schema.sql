CREATE TABLE locations (
  location_id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL UNIQUE,
  latitude DECIMAL(9,6) NOT NULL,
  longitude DECIMAL(9,6) NOT NULL,
  is_urban BOOLEAN NOT NULL,
  urban_pair_id INTEGER -- NULL for rural locations
);

CREATE TABLE temperature_data (
  location_id INTEGER REFERENCES locations(location_id),
  timestamp TIMESTAMP NOT NULL,
  temperature DECIMAL(5,2) NOT NULL, -- in °C
  collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (location_id, timestamp)  -- Include timestamp in PRIMARY KEY
) PARTITION BY RANGE (timestamp);

-- Create partitions for historical data
CREATE TABLE temperature_data_2023_q3 PARTITION OF temperature_data
  FOR VALUES FROM ('2023-07-01') TO ('2023-10-01');
CREATE TABLE temperature_data_2023_q4 PARTITION OF temperature_data
  FOR VALUES FROM ('2023-10-01') TO ('2024-01-01');

-- Current partitions and future partitions
CREATE TABLE temperature_data_2024_q1 PARTITION OF temperature_data
  FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
CREATE TABLE temperature_data_2024_q2 PARTITION OF temperature_data
  FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
CREATE TABLE temperature_data_2024_q3 PARTITION OF temperature_data
  FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');
CREATE TABLE temperature_data_2024_q4 PARTITION OF temperature_data
  FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');
CREATE TABLE temperature_data_2025_q1 PARTITION OF temperature_data
  FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');
CREATE TABLE temperature_data_2025_q2 PARTITION OF temperature_data
  FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');
CREATE TABLE temperature_data_2025_q3 PARTITION OF temperature_data
  FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');