from datetime import datetime, timedelta
import logging

import requests
import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable

# Setup logger for this module
logger = logging.getLogger(__name__)


# Cities in Bangladesh (you can add more)
CITIES = [
    "Dhaka,BD",
    "Chittagong,BD",
    "Sylhet,BD",
    "Rajshahi,BD",
    "Khulna,BD",
]

# OpenWeather base URL
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_weather(**kwargs):
    """Fetch current weather for Bangladeshi cities and push to XCom."""
    logger.info("Starting fetch_weather task")

    api_key = Variable.get("OPENWEATHER_API_KEY")
    logger.info("Successfully read OPENWEATHER_API_KEY from Airflow Variables")

    records = []

    for city in CITIES:
        logger.info("Requesting weather for city=%s", city)
        try:
            resp = requests.get(
                BASE_URL,
                params={
                    "q": city,
                    "appid": api_key,
                    "units": "metric",
                },
                timeout=10,
            )
            logger.info("Received response for %s with status_code=%s", city, resp.status_code)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error("HTTP error while fetching weather for %s: %s", city, e, exc_info=True)
            # You can choose to continue or raise; for now we skip this city
            continue

        data = resp.json()
        logger.debug("Raw JSON for %s: %s", city, data)

        try:
            record = {
                "city": city.split(",")[0],
                "country": "BD",
                "timestamp_utc": datetime.utcfromtimestamp(data["dt"]),
                "temp_c": data["main"]["temp"],
                "feels_like_c": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "weather_main": data["weather"][0]["main"],
                "weather_desc": data["weather"][0]["description"],
                "wind_speed": data["wind"]["speed"],
            }
            records.append(record)
            logger.info(
                "Parsed weather for %s: temp=%.2f°C, humidity=%s%%, weather=%s",
                record["city"],
                record["temp_c"],
                record["humidity"],
                record["weather_main"],
            )
        except KeyError as e:
            logger.error("Missing key %s in API response for city=%s", e, city, exc_info=True)

    logger.info("Fetched %d valid weather records", len(records))

    # Save data in XCom
    ti = kwargs["ti"]
    ti.xcom_push(key="weather_records", value=records)
    logger.info("Pushed %d records to XCom under key 'weather_records'", len(records))


def load_to_postgres(**kwargs):
    """Create table if not exists and insert weather records into Postgres."""
    logger.info("Starting load_to_postgres task")

    ti = kwargs["ti"]
    records = ti.xcom_pull(key="weather_records", task_ids="fetch_weather_task")

    if not records:
        logger.warning("No records received from XCom; nothing to insert into Postgres")
        return

    logger.info("Pulled %d records from XCom", len(records))

    conn = None
    cur = None

    try:
        logger.info("Connecting to Postgres host=weather-db db=weather user=weather")
        conn = psycopg2.connect(
            host="weather-db",
            port=5432,
            user="weather",
            password="weather",
            dbname="weather",
        )
        cur = conn.cursor()
        logger.info("Postgres connection established successfully")

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS bd_weather_hourly (
            id SERIAL PRIMARY KEY,
            city VARCHAR(50),
            country VARCHAR(5),
            timestamp_utc TIMESTAMP,
            temp_c NUMERIC,
            feels_like_c NUMERIC,
            humidity INTEGER,
            pressure INTEGER,
            weather_main VARCHAR(50),
            weather_desc VARCHAR(100),
            wind_speed NUMERIC
        );
        """
        logger.info("Creating table bd_weather_hourly if it does not exist")
        cur.execute(create_table_sql)

        insert_sql = """
        INSERT INTO bd_weather_hourly (
            city, country, timestamp_utc, temp_c, feels_like_c,
            humidity, pressure, weather_main, weather_desc, wind_speed
        )
        VALUES (
            %(city)s, %(country)s, %(timestamp_utc)s, %(temp_c)s, %(feels_like_c)s,
            %(humidity)s, %(pressure)s, %(weather_main)s, %(weather_desc)s, %(wind_speed)s
        );
        """

        inserted_count = 0
        for rec in records:
            cur.execute(insert_sql, rec)
            inserted_count += 1

        conn.commit()
        logger.info("Inserted %d records into bd_weather_hourly and committed transaction", inserted_count)

    except Exception as e:
        logger.error("Error while inserting into Postgres: %s", e, exc_info=True)
        if conn:
            conn.rollback()
            logger.info("Rolled back transaction due to error")
        # Re-raise so task is marked as failed
        raise
    finally:
        if cur:
            cur.close()
            logger.info("Closed Postgres cursor")
        if conn:
            conn.close()
            logger.info("Closed Postgres connection")


# Default DAG arguments
default_args = {
    "owner": "asrar",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bd_weather_pipeline",
    default_args=default_args,
    description="Bangladesh weather data pipeline using OpenWeather + Postgres",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",   # Airflow 3.x: use `schedule` instead of `schedule_interval`
    catchup=False,
    tags=["bangladesh", "weather", "data-engineering"],
) as dag:

    fetch_weather_task = PythonOperator(
        task_id="fetch_weather_task",
        python_callable=fetch_weather,
    )

    load_to_postgres_task = PythonOperator(
        task_id="load_to_postgres_task",
        python_callable=load_to_postgres,
    )

    fetch_weather_task >> load_to_postgres_task
