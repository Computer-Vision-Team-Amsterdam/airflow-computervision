from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

try:
    username = Variable.get("DecosUsername")
    password = Variable.get("DecosPassword")
    url = Variable.get("DecosURL")
except Exception as e:
    print(e)

print("Test in prd.")
try:
    print(url.split(".")[0])
except Exception as e:
    print(e)


def login_test(**context):
    values = {'username': username,
              'password': password}

    try:
        import requests
        r = requests.post(url, data=values)
        print(r.content)
    except Exception as e:
        print(e)

def login_test_twee(**context):
    try:
        import requests
        from urllib3.exceptions import InsecureRequestWarning
        from urllib3 import disable_warnings

        disable_warnings(InsecureRequestWarning)
        page = requests.get(url, verify=False)
        print(page.text.encode('utf8'))

        print(page.content)
    except Exception as e:
        print(e)

def login_test_drie(**context):
    try:
        from urllib.request import urlopen
        print(urlopen(url).read())

        import requests
        page = requests.get(url, verify=False)
        print(page.text.encode('utf8'))

        print(page.content)
    except Exception as e:
        print(e)

with DAG(dag_id='bash_dag', schedule_interval=None, start_date=datetime(2022, 11, 24), catchup=False) as dag:
    telnet_test = BashOperator(
        task_id='bash_task',
        bash_command="echo -e '\x1dclose\x0d' | telnet sfte.amsterdam.nl 22",
        dag=dag
    )

    login_test = PythonOperator(
        task_id='login_test',
        python_callable=login_test,
        provide_context=True,
        dag=dag
    )

    login_test_twee = PythonOperator(
        task_id='login_test_twee',
        python_callable=login_test_twee,
        provide_context=True,
        dag=dag
    )

    login_test_drie = PythonOperator(
        task_id='login_test_drie',
        python_callable=login_test_drie,
        provide_context=True,
        dag=dag
    )

    flow = telnet_test >> login_test >> login_test_twee >> login_test_drie
