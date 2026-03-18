from airflow.decorators import dag
from pendulum import datetime,duration
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from include.datasets import cocktail_dataset

@dag(
    start_date=datetime(2024, 6, 1), 
    schedule=[cocktail_dataset], 
    catchup=False,
    description="E-commerce DAG to extract, transform and load data",
    tags=["commerce", "etl"],     
    default_args={"retries":1},
    dagrun_timeout=duration(minutes=60),
    max_consecutive_failed_dag_runs=3
)
def ecom():
    task1 = EmptyOperator(task_id='task1')
    
ecom()