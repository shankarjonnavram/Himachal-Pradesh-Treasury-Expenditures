from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from budget_dataset.database.loading_to_sqllite import df_to_sqllite


project_path = '/Users/tnluser/airflow/dags/budget_dataset'


default_args = {
    'owner' : 'Shankar',
    'retries' :2,
    'retry_delay' : timedelta(minutes=1)
}

dag = DAG("scapy_project_treasury_expenditure",
          default_args = default_args,
          tags = ['scraping'],
          start_date = datetime(2023,7,15),
          schedule_interval = None,
          catchup = False
          )

web_to_csv = BashOperator(
    task_id='web_to_csv',
    bash_command=f'cd {project_path} && scrapy crawl budget',
    dag=dag)

csv_to_sqldb = PythonOperator (
    task_id = 'csv_to_sqldb',
    python_callable=df_to_sqllite,
    op_kwargs = {'project_path':f'{project_path}'},
    dag=dag
)

web_to_csv >> csv_to_sqldb

