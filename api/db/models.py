from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Location(Base):
    __tablename__ = "locations"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)

    sensors = relationship("Sensor", back_populates="location")

class Sensor(Base):
    __tablename__ = "sensors"

    id = Column(Integer, primary_key=True, index=True)
    parameter = Column(String)
    unit = Column(String)
    location_id = Column(Integer, ForeignKey("locations.id"))

    location = relationship("Location", back_populates="sensors")
    measurements = relationship("Measurement", back_populates="sensor")

class Measurement(Base):
    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"))
    datetime = Column(DateTime)
    value = Column(Float)

    sensor = relationship("Sensor", back_populates="measurements")

class Parameter(Base):
    __tablename__ = "parameters"

    id = Column(Integer, primary_key=True, index=True)

