from airflow.decorators import dag
from datetime import datetime

@dag(
    start_date=datetime(2026, 3, 5),
    schedule='@daily',
    catchup=False,
    tags=['taskflow']
)

def taskflow():
    
    def task_a():
        print("This is task A")
        return 42
    def task_b(value):
        print("This is task B")
        return (value)
    
    task_b(task_a())
    
taskflow()