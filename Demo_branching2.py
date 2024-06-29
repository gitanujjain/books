from datetime import datetime, timedelta

import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator


def fail(**_):
    raise Exception("failure!")


with airflow.DAG(
        'demo_branching_2',
        dagrun_timeout=timedelta(hours=1),
        schedule_interval="@monthly",
        start_date=datetime(2017, 8, 1),
        end_date=datetime(2017, 9, 1),
        default_args={
            'owner': 'demo',
            'provide_context': True,
        }) as dag:

    step1 = DummyOperator(task_id='step1')

    branch1 = DummyOperator(task_id='branch1')
    branch2 = PythonOperator(task_id='branch2', python_callable=fail)

    final_step = DummyOperator(task_id='final_step', trigger_rule='one_success')

    def choose_branch(execution_date, **_):
        if execution_date == datetime(2017, 8, 1):
            return branch1.task_id
        else:
            return branch2.task_id

    branching_step = BranchPythonOperator(
        task_id='branching_step',
        python_callable=choose_branch
    )

    dag >> step1 >> branching_step

    branching_step >> branch1 >> final_step
    branching_step >> branch2 >> final_step

