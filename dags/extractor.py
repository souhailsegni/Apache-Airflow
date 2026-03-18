from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.python import PythonOperator
from include.datasets import cocktail_dataset


def get_coktails(ti=None):
    import requests
    api_url = 'https://www.thecocktaildb.com/api/json/v1/1/random.php'
    response = requests.get(api_url)
    
    with open(cocktail_dataset, 'wb') as f:
            f.write(response.content)
            
    ti.xcom_push(key='cocktail_data', value=len(response.content))
    
def _check_size(ti=None):
    size = ti.xcom_pull(key='cocktail_data', task_ids='get_coktail')
    print(size)

@dag(start_date=datetime(2024, 6, 1), schedule='@daily', catchup=False)

def extractor():
    
    get_coktail= PythonOperator(
        task_id='get_coktail',
        python_callable=get_coktails,
        outlets=[cocktail_dataset]
    )
    check_size = PythonOperator(
        task_id='check_size',
        python_callable=_check_size,
    )
    get_coktail >> check_size
    
extractor()