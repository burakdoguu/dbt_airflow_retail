import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from cosmos.providers.dbt.task_group import DbtTaskGroup

@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def basic_cosmos_task_group() -> None:
    start = EmptyOperator(task_id='start')

    jaffle_shop = DbtTaskGroup(
        dbt_root_path="/opt/airflow/dbt",
        dbt_project_name="retail",
        conn_id="postgres",
        profile_args={"schema": "public"},
    )

    end = EmptyOperator(task_id='end')

    start >> jaffle_shop >> end

basic_cosmos_task_group()