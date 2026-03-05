from airflow.decorators import dag
from datetime import datetime, timedelta

@dag(
    start_date=datetime(2026, 3, 5),
    schedule='@daily',
    catchup=False,
    tags=['stock_market']
)

def stock_market():
    pass

stock_market()