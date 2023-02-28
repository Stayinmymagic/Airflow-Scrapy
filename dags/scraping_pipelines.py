import json
import pathlib
import subprocess
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import pymysql
from datetime import datetime
dag = DAG(
    dag_id="scrapying_pipelines",
    description="Web scraping forbes news",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
    max_active_runs=1 
)


scrape_proxy = BashOperator(
    task_id="scrape_proxy",
    bash_command = '{{"bash ./airflow-tutorial/airflow/bin/run_proxy"}}',
    dag=dag
)


def _check_proxy():
    with open('./airflow-tutorial/airflow/Web-Scraper-forbes/get_proxy/proxy.json') as f:
        proxy = json.load(f)
        if len(proxy) > 5:
            return "scrape_news"
        else:
            return "stop_task"
        
branch_op = BranchPythonOperator(
    task_id = "check_proxy",
    python_callable = _check_proxy
)

continue_op = BashOperator(
    task_id="scrape_news",
    bash_command = '{{"bash ./airflow-tutorial/airflow/bin/run_reuters"}}',
    dag=dag)

stop_op = EmptyOperator(task_id="stop_task", dag=dag)


def _save2db():
    with open('./airflow-tutorial/airflow/Web-Scraper-forbes/reuters/news.json') as f:
        news = json.load(f)
    connect = pymysql.connect(
    host='localhost',
    db="airflow_db",
    user='root',
    passwd='',
    charset='utf8'
    )

    cursor = connect.cursor()

    sql_insert = 'INSERT INTO news (link, date, title, topic, summary)VALUES(%s,%s,%s,%s,%s)'
    for i, item in enumerate(news):
        link = item.get("link", None)
        try:
            date = datetime.strptime(item.get("date", None), '%Y-%m-%d')
        except:
            date = None
        title = item.get("title", None)
        topic = item.get("topic", None)
        summary = item.get("summary", None)
    connect.ping(reconnect = True)
    cursor.execute(sql_insert, (link, date, title, topic, summary))
    connect.commit()
    connect.close()

save2db = PythonOperator(
    task_id = "savetodb",
    python_callable = _save2db,
    dag = dag,
    trigger_rule=TriggerRule.NONE_FAILED

)


scrape_proxy >>  branch_op >> [continue_op, stop_op] >> save2db


