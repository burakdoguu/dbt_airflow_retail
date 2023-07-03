from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
import pandas as pd

from pendulum import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from cosmos.providers.dbt.core.operators import DbtRunOperationOperator, DbtSeedOperator


PATH_TO_DBT_PROJECT = "/opt/airflow/dbt/retail"
ORDER_JSONPATH = "data_json/orders.json"
CONNECTION_ID = "postgres"
SCHEMA_NAME = 'dwh'
DB_NAME = 'dwh'
DBT_PROJECT_NAME = "retail"



def order_json_to_csv():

    df_read_json = pd.read_json(ORDER_JSONPATH, lines=True)
    df_explode_json = df_read_json.explode("lineitems")
    df_concat = pd.concat([df_explode_json.drop(['lineitems'], axis=1), df_explode_json['lineitems'].apply(pd.Series)], axis=1)
    df_orders = df_concat[['event', 'messageid', 'userid', 'productid', 'quantity', 'orderid']]
    df_orders.to_csv("/opt/airflow/dbt/retail/seeds/orders.csv")

with DAG(
    dag_id="import-seeds",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    
    order_csv = PythonOperator(
        task_id='order_csv',
        python_callable=order_json_to_csv
    )

    with TaskGroup(group_id="drop_seeds_if_exist") as drop_seeds:
        for seed in ["product_category_map", "orders"]:
            DbtRunOperationOperator(
                task_id=f"drop_{seed}_if_exists",
                macro_name="drop_table",
                args={"table_name": seed},
                project_dir=f"/opt/airflow/dbt/retail",
                profile_args={"schema": "public"},
                #dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
                conn_id="postgres",
            )

    create_seeds = DbtSeedOperator(
        task_id=f"retail_seed",
        #project_dir=DBT_ROOT_PATH / "jaffle_shop",
        project_dir=f"/opt/airflow/dbt/retail",
        profile_args={"schema": "public"},
        #dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        conn_id="postgres",
        outlets=[Dataset(f"SEED://RETAIL")],
     )

    order_csv >> drop_seeds >> create_seeds

