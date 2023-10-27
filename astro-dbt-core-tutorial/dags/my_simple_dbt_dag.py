"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# adjust for other database types
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pendulum import datetime
import os
from airflow.utils.task_group import TaskGroup

YOUR_NAME = "GPARLADE"
CONNECTION_ID = "db_conn"
DB_NAME = "POC_AIRFLOW_DBT"
SCHEMA_NAME = "Bronze_layer"
MODEL_TO_QUERY = "model2"
# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/my_simple_dbt_project"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": SCHEMA_NAME,"login": YOUR_NAME ,"account": "wk09606.eu-west-3.aws","warehouse": "PC_DBT_WH","database": DB_NAME,"role": "PC_DBT_ROLE"}
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    params={"my_name": YOUR_NAME},
)
def my_simple_dbt_dag():
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "vars": '{"my_name": {{ params.my_name }} }',
        },
        default_args={"retries": 2},
    )

    query_table = SnowflakeOperator(
        task_id="query_table",
        snowflake_conn_id=CONNECTION_ID,
        sql=f"SELECT * FROM {DB_NAME}.{SCHEMA_NAME}.{MODEL_TO_QUERY}",
    )

    transform_data >> query_table


my_simple_dbt_dag()