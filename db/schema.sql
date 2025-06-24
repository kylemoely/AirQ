CREATE TABLE IF NOT EXISTS countries (
	id INTEGER PRIMARY KEY,
	name TEXT
	);

CREATE TABLE IF NOT EXISTS parameters (
	id INTEGER PRIMARY KEY,
	units TEXT,
	name TEXT,
	description TEXT
	);

CREATE TABLE IF NOT EXISTS locations (
	id INTEGER PRIMARY KEY,
	name TEXT,
	latitude DOUBLE PRECISION,
	longitude DOUBLE PRECISION,
	country_id INTEGER,
	FOREIGN KEY (country_id) REFERENCES countries(id)
	);

CREATE TABLE IF NOT EXISTS sensors (
	id INTEGER PRIMARY KEY,
	name TEXT,
	location_id INTEGER,
	parameter_id INTEGER,
	FOREIGN KEY (location_id) REFERENCES locations(id),
	FOREIGN KEY (parameter_id) REFERENCES parameters(id)
	);

CREATE TABLE IF NOT EXISTS measurements (
	id INTEGER PRIMARY KEY,
	datetime TIMESTAMP NOT NULL,
	value DOUBLE PRECISION NOT NULL,
	sensor_id INTEGER NOT NULL,
	FOREIGN KEY (sensor_id) REFERENCES sensors(id)
	);
