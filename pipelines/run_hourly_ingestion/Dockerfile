FROM python:3.11-slim

WORKDIR /usr/local/app

COPY ./etl/ etl/
COPY ./db/ db/
COPY ./pipelines/run_hourly_ingestion/ .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "run.py"]
