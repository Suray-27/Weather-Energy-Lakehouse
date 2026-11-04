import json
import subprocess
import time
from datetime import datetime, timezone

from airflow.sdk import dag, task, Param
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.bash import BashOperator


RESOURCE_GROUP = "energyproject"
FACTORY_NAME = "weather-energy-adf"
PIPELINE_NAME = "PL_Weather_Master"


@dag(
    dag_id="weather_lakehouse_orchestration",
    description="Orchestrate Weather Energy Lakehouse ingestion through ADF",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    params={
    "api_mode": Param(
        "forecast",
        type="string",
        enum=["forecast", "archive"],
    ),
    "start_date": Param("", type="string"),
    "end_date": Param("", type="string"),
},
    tags=["weather", "azure", "adf"],
)
def weather_lakehouse_orchestration():

    @task
    def trigger_adf_pipeline():
        from airflow.sdk import get_current_context

        context = get_current_context()
        params = context["params"]
        print("AIRFLOW API MODE:", params["api_mode"])
        print("AIRFLOW START DATE:", params["start_date"])
        print("AIRFLOW END DATE:", params["end_date"])

        api_mode = params["api_mode"]
        start_date = params["start_date"]
        end_date = params["end_date"]

        if api_mode == "archive":

            if not start_date or not end_date:
                raise ValueError(
                    "Archive mode requires both start_date and end_date."
                )

            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()

            if start > end:
                raise ValueError("Start date cannot be after end date.")

        elif api_mode == "forecast":

            start_date = ""
            end_date = ""

            print("Forecast mode: Automatically fetching next 7 days.")

        parameters = {
            "p_api_mode": api_mode,
            "p_start_date": start_date,
            "p_end_date": end_date,
            "p_recovery_run_id": "",
        }

        command = [
            "az", "datafactory", "pipeline", "create-run",
            "--resource-group", RESOURCE_GROUP,
            "--factory-name", FACTORY_NAME,
            "--name", PIPELINE_NAME,
            "--parameters", json.dumps(parameters),
            "--output", "json",
        ]

        print("ADF PARAMETERS:", json.dumps(parameters, indent=2))

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
        )

        run_id = json.loads(result.stdout)["runId"]

        print(f"ADF pipeline triggered. Run ID: {run_id}")
        return run_id

    @task
    def wait_for_adf_completion(adf_run_id: str):
        timeout_seconds = 60 * 60
        poll_interval_seconds = 30
        start_time = time.monotonic()

        while time.monotonic() - start_time < timeout_seconds:
            command = [
                "az", "datafactory", "pipeline-run", "show",
                "--resource-group", RESOURCE_GROUP,
                "--factory-name", FACTORY_NAME,
                "--run-id", adf_run_id,
                "--output", "json",
            ]

            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True,
            )

            run_details = json.loads(result.stdout)
            status = run_details["status"]

            print(f"ADF run {adf_run_id}: {status}")

            if status == "Succeeded":
                print("ADF ingestion completed successfully.")
                return status

            if status in ["Failed", "Cancelled"]:
                raise RuntimeError(
                    f"ADF pipeline ended with status: {status}"
                )

            time.sleep(poll_interval_seconds)

        raise TimeoutError(
            f"ADF pipeline did not finish within {timeout_seconds} seconds."
        )

    # Databricks job: Bronze to Silver
    run_databricks_job = DatabricksRunNowOperator(
        task_id="run_bronze_to_silver_databricks",
        databricks_conn_id="databricks_default",
        job_id=906432633226296,
        notebook_params={
        "storage_account": "{{ var.value.STORAGE_ACCOUNT }}",
        "bronze_container": "{{ var.value.BRONZE_CONTAINER }}",
        "blob_prefix": "{{ var.value.BLOB_PREFIX }}",
            },
    wait_for_termination=True,
    polling_period_seconds=30,
    )

    dbt_run_gold = BashOperator(
    task_id="dbt_run_gold",
    bash_command="""
        set -e
        source /home/xander/dbt-project/dbt-venv/bin/activate
        cd /mnt/d/Projects/Weather-Energy-Lakehouse/weather_energy_dbt
        export DBT_PROFILES_DIR=/home/xander/.dbt
        dbt run
    """,
)

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command="""
            set -e
            source /home/xander/dbt-project/dbt-venv/bin/activate
            cd /mnt/d/Projects/Weather-Energy-Lakehouse/weather_energy_dbt
            export DBT_PROFILES_DIR=/home/xander/.dbt
            dbt test
        """,
    )

    adf_run_id = trigger_adf_pipeline()
    adf_completion = wait_for_adf_completion(adf_run_id)
    adf_completion >> run_databricks_job >> dbt_run_gold >> dbt_test_gold

weather_lakehouse_orchestration()
