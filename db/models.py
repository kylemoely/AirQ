from datetime import datetime
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class Location(Base):
    __tablename__ = "locations"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    latitude: Mapped[float]
    longitude: Mapped[float]
    country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"))

class Country(Base):
    __tablename__ = "countries"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

class Parameter(Base):
    __tablename__ = "parameters"

    id: Mapped[int] = mapped_column(primary_key=True)
    units: Mapped[str]
    name: Mapped[str]
    description: Mapped[str]

class Sensor(Base):
    __tablename__ = "sensors"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    location_id: Mapped[int] = mapped_column(ForeignKey("locations.id"))
    parameter_id : Mapped[int] = mapped_column(ForeignKey("parameters.id"))

class Measurement(Base):
    __tablename__ = "measurements"

    id: Mapped[int] = mapped_column(primary_key=True)
    datetime: Mapped[datetime]
    value: Mapped[float]
    sensor_id: Mapped[int] = mapped_column(ForeignKey("sensors.id"))
