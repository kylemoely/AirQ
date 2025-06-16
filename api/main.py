from fastapi import FastAPI
from api.routes import locations, countries, parameters

app = FastAPI(
        title="AirQ API",
        description="An API for serving air quality metrics from OpenAQ data.",
        version="1.0.0"
    )

app.include_router(locations.router, prefix="/locations", tags=["Locations"])
app.invlude_router(measurements.router, prefix="/measurements", tags=["Measurements"])

