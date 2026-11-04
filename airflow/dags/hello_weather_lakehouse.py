from airflow.sdk import dag, task
from datetime import datetime, timezone


@dag(
    dag_id="hello_weather_lakehouse",
    description="First test DAG for Weather Energy Lakehouse",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["weather", "learning"],
)
def hello_weather_lakehouse():
    @task
    def verify_airflow():
        print("Airflow is successfully executing tasks!")
        print("Weather & Energy Lakehouse orchestration is ready.")

    verify_airflow()


hello_weather_lakehouse()
