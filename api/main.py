from fastapi import FastAPI
from api.routes import locations, countries, parameters, sensors, measurements

app = FastAPI(
        title="AirQ API",
        description="An API for serving air quality metrics from OpenAQ data.",
        version="1.0.0"
    )

app.include_router(locations.router, prefix="/locations", tags=["Locations"])
app.include_router(measurements.router, prefix="/measurements", tags=["Measurements"])
app.include_router(countries.router, prefix="/countries", tags=["Countries"])
app.include_router(parameters.router, prefix="/parameters", tags=["Paramters"])
app.include_router(sensors.router, prefix="/sensors", tags=["Sensors"])

