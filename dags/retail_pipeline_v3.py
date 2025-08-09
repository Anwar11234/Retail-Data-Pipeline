from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag
def retail_pipeline_v3():

    create_tables = SQLExecuteQueryOperator(
        task_id = 'create_tables',
        conn_id = 'postgres',
        sql='sql/create_tables.sql'
    )

    @task
    def get_data():
        import pandas as pd

        df = pd.read_excel('/opt/airflow/data/Online_Retail.xlsx')
        df.to_csv('/tmp/raw.csv', index=False)

        hook = PostgresHook(postgres_conn_id='postgres')
        

        hook.copy_expert(
            sql='COPY stage_raw FROM STDIN WITH CSV HEADER',
            filename='/tmp/raw.csv'
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

    create_tables >> get_data() >> [update_products, update_customers] >> update_sales >> update_returns


retail_pipeline_v3()