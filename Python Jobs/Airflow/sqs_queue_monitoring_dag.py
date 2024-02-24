from airflow import DAG, settings
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sqs_message_monitoring',
    default_args=default_args,
    description='Monitor SQS queue for new messages',
    schedule_interval=timedelta(minutes=5),
)

aws_conn_id = Variable.get('aws_conn_id')
queue_name = Variable.get('queue_name')

sqs_sensor_task = SQSSensor(
    task_id='sqs_sensor_task',
    aws_conn_id=aws_conn_id,
    queue_name=queue_name,
    timeout=60,  # Timeout for the sensor in seconds
    dag=dag,
)

process_message_task = DummyOperator(
    task_id='process_message_task',
    dag=dag,
)

sqs_sensor_task >> process_message_task
