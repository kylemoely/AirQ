CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS countries (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	api_countryId INTEGER NOT NULL,
	name TEXT
	);

CREATE TABLE IF NOT EXISTS parameters (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	api_parameterId INTEGER NOT NULL,
	units TEXT,
	name TEXT,
	description TEXT
	);

CREATE TABLE IF NOT EXISTS locations (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	api_locationId INTEGER NOT NULL,
	name TEXT,
	city TEXT,
	latitude DOUBLE PRECISION,
	longitude DOUBLE PRECISION,
	countryId UUID,
	FOREIGN KEY (countryId) REFERENCES countries(id)
	);

CREATE TABLE IF NOT EXISTS sensors (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	api_sensorId INTEGER NOT NULL,
	name TEXT,
	locationId UUID,
	parameterId UUID,
	FOREIGN KEY (locationId) REFERENCES locations(id),
	FOREIGN KEY (parameterId) REFERENCES parameters(id)
	);

CREATE TABLE IF NOT EXISTS measurements (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	datetime TIMESTAMP NOT NULL,
	value DOUBLE PRECISION NOT NULL,
	sensorId UUID,
	FOREIGN KEY (sensorId) REFERENCES sensors(id)
	);
