# Bangladesh Weather Pipeline

An Apache Airflow data pipeline that fetches real-time weather data for major Bangladeshi cities and stores it in PostgreSQL.

## Overview

This project automates the collection of weather data using the OpenWeather API and orchestrates the entire workflow with Apache Airflow. The pipeline runs hourly and captures comprehensive weather metrics for five major cities in Bangladesh.
<img width="841" height="473" alt="image" src="https://github.com/user-attachments/assets/ef05524d-cbe7-4bd2-b07a-6ceebf414a8f" />
<img width="841" height="473" alt="image" src="https://github.com/user-attachments/assets/1f32b6f6-dc0a-4820-a3b0-03ee1ecd6bc7" />
<img width="1252" height="314" alt="image" src="https://github.com/user-attachments/assets/8e6ea39d-736c-4ab0-b567-a2dda15e8b2a" />


## Features

- **Hourly Weather Collection**: Automatically fetches weather data every hour
- **Multi-City Support**: Tracks weather for Dhaka, Chittagong, Sylhet, Rajshahi, and Khulna
- **PostgreSQL Integration**: Persists weather data in a relational database
- **Comprehensive Metrics**: Collects temperature, humidity, pressure, wind speed, and weather conditions
- **Error Handling**: Robust error handling with retry logic and detailed logging
- **Docker Support**: Runs in Docker containers for easy deployment

## Project Structure

```
.
├── compose.yaml              # Docker Compose configuration
├── config/
│   └── airflow.cfg          # Airflow configuration
├── dags/
│   └── bd_weather_dag.py    # Main Airflow DAG
├── logs/                     # Airflow task logs
├── plugins/                  # Custom plugins directory
├── .env                      # Environment variables (local)
├── .env.example             # Example environment variables
└── README.md                # This file
```

## Prerequisites

- Docker & Docker Compose
- OpenWeather API key (get one at https://openweathermap.org/api)

## Setup & Installation

### 1. Clone the Repository

```bash
git clone https://github.com/asrar000/bangladesh_weather_pipeline.git
cd bangladesh_weather_pipeline/airflow
```

### 2. Configure Environment Variables

Copy the example environment file and add your OpenWeather API key:

```bash
cp .env.example .env
```

Edit `.env` and replace the placeholder with your actual API key:

```bash
AIRFLOW_UID=your (id -u)
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_VAR_OPENWEATHER_API_KEY=your_actual_api_key
```

### 3. Start the Services

```bash
docker compose up -d
```

This will start:
- Airflow WebServer (http://localhost:8080)
- Airflow Scheduler
- PostgreSQL Database
- Redis Broker (if configured)

### 4. Access Airflow

- **URL**: http://localhost:8080
- **Default Username**: airflow
- **Default Password**: airflow

## DAG Overview

### bd_weather_pipeline

**Schedule**: Runs hourly (`@hourly`)  
**Owner**: asrar  
**Retries**: 1 (with 5-minute delay)

#### Tasks

1. **fetch_weather_task**
   - Fetches current weather data from OpenWeather API for each city
   - Extracts key metrics: temperature, humidity, pressure, wind speed, weather conditions
   - Pushes results to Airflow XCom for downstream tasks

2. **load_to_postgres_task**
   - Retrieves weather records from XCom
   - Creates `bd_weather_hourly` table if it doesn't exist
   - Inserts weather records into PostgreSQL
   - Includes transaction management and rollback on failure

#### Task Dependency

```
fetch_weather_task >> load_to_postgres_task
```

## Database Schema

The pipeline creates and populates the `bd_weather_hourly` table:

```sql
CREATE TABLE bd_weather_hourly (
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
```

## Monitored Cities

The pipeline collects data for:
- Dhaka
- Chittagong
- Sylhet
- Rajshahi
- Khulna

To add more cities, edit the `CITIES` list in `dags/bd_weather_dag.py`:

```python
CITIES = [
    "Dhaka,BD",
    "Chittagong,BD",
    "Sylhet,BD",
    "Rajshahi,BD",
    "Khulna,BD",
    # Add more cities here
]
```

## Configuration



Modify the connection details in `load_to_postgres_task` function if needed.

### API Configuration

- **Base URL**: https://api.openweathermap.org/data/2.5/weather
- **Units**: Metric (Celsius)
- **Request Timeout**: 10 seconds



## Error Handling

- **API Errors**: Individual city failures don't stop the pipeline; the task continues with other cities
- **Database Errors**: Transaction rollback on failure; task is marked as failed
- **Retries**: Failed tasks retry once after 5 minutes
- **Timeouts**: 10-second timeout on API requests to prevent hanging

## Troubleshooting

### Pipeline Not Running

1. Check the Airflow Scheduler is running: `docker-compose ps`
2. Review logs in Airflow UI or `logs/` directory
3. Verify API key is correctly set in `.env` file

### Database Connection Errors

1. Ensure PostgreSQL container is running: `docker-compose logs weather-db`
2. Verify credentials in `load_to_postgres_task`
3. Check network connectivity between Airflow and database containers

### Missing Weather Data

1. Verify OpenWeather API key is valid
2. Check API rate limits (free tier: 60 calls/minute)
3. Review task logs for specific city failures


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the MIT License.

## Author

- **Asrar Ahmed** - Initial work - [asrar000](https://github.com/asrar000)

## Support

For issues, questions, or suggestions, please create an issue on the GitHub repository.
