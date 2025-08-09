from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from datetime import datetime
from pathlib import Path
import pandas as pd

@dag(
    schedule="@daily",
    start_date=datetime(2010, 12, 1),
    end_date=datetime(2011, 12, 9),
    catchup=True,
    max_active_runs=1
)
def retail_pipeline_v4():

    create_tables = SQLExecuteQueryOperator(
        task_id = 'create_tables',
        conn_id = 'postgres',
        sql='sql/create_tables.sql'
    )

    @task
    def get_data(**context):
        date_str = context["dag_run"].logical_date.strftime("%Y-%m-%d")

        file_path = Path(f"/opt/airflow/data/daily_files/Online_Retail_{date_str}.xlsx")

        if not file_path.exists():
            raise AirflowSkipException(f"No file found for {date_str}, skipping run.")

        df = pd.read_excel(file_path)

        tmp_file = f"/tmp/raw_{date_str}.csv"
        df.to_csv(tmp_file, index=False)

        hook = PostgresHook(postgres_conn_id='postgres')
        hook.copy_expert(
            sql='COPY stage_raw FROM STDIN WITH CSV HEADER',
            filename=tmp_file
        )
        
    update_customers = SQLExecuteQueryOperator(
        task_id = 'update_customers', 
        conn_id='postgres',
        sql='sql/update_customers.sql'
    )

    update_products = SQLExecuteQueryOperator(
        task_id = 'update_products', 
        conn_id='postgres',
        sql='sql/update_products.sql'
    )

    update_sales = SQLExecuteQueryOperator(
        task_id = 'update_sales', 
        conn_id='postgres',
        sql='sql/update_fact_sales.sql'
    )

    update_returns = SQLExecuteQueryOperator(
        task_id = 'update_returns', 
        conn_id='postgres',
        sql='sql/update_fact_returns.sql'
    )

    create_tables >> get_data() >> [update_products, update_customers]

    [update_products, update_customers] >> update_sales
    [update_products, update_customers] >> update_returns



retail_pipeline_v4()