CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS countries (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	api_countryid INTEGER NOT NULL,
	name TEXT
	);

CREATE TABLE IF NOT EXISTS parameters (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	api_parameterid INTEGER NOT NULL,
	units TEXT,
	name TEXT,
	description TEXT
	);

CREATE TABLE IF NOT EXISTS locations (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	api_locationid INTEGER NOT NULL,
	name TEXT,
	city TEXT,
	latitude DOUBLE PRECISION,
	longitude DOUBLE PRECISION,
	countryid UUID,
	FOREIGN KEY (countryId) REFERENCES countries(id)
	);

CREATE TABLE IF NOT EXISTS sensors (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	api_sensorid INTEGER UNIQUE NOT NULL,
	name TEXT,
	locationid UUID,
	parameterid UUID,
	FOREIGN KEY (locationId) REFERENCES locations(id),
	FOREIGN KEY (parameterId) REFERENCES parameters(id)
	);

CREATE TABLE IF NOT EXISTS measurements (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	datetime TIMESTAMP NOT NULL,
	value DOUBLE PRECISION NOT NULL,
	api_sensorid INTEGER NOT NULL,
	FOREIGN KEY (api_sensorid) REFERENCES sensors(api_sensorid)
	);
