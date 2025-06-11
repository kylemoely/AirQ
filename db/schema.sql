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
	latitude DOUBLE PRECISION,
	longitude DOUBLE PRECISION,
	api_countryid UUID,
	FOREIGN KEY (api_countryId) REFERENCES countries(api_countryid)
	);

CREATE TABLE IF NOT EXISTS sensors (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	api_sensorid INTEGER UNIQUE NOT NULL,
	name TEXT,
	api_locationid UUID,
	api_parameterid UUID,
	FOREIGN KEY (api_locationId) REFERENCES locations(api_locationid),
	FOREIGN KEY (api_parameterId) REFERENCES parameters(api_parameterid)
	);

CREATE TABLE IF NOT EXISTS measurements (
	id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	datetime TIMESTAMP NOT NULL,
	value DOUBLE PRECISION NOT NULL,
	api_sensorid INTEGER NOT NULL,
	FOREIGN KEY (api_sensorid) REFERENCES sensors(api_sensorid)
	);
