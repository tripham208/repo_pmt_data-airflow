"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='example_bash_operator_test',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

run_this_last = EmptyOperator(
    task_id='run_this_last',
    dag=dag,
)

# [START howto_operator_bash]
run_this = BashOperator(
    task_id='run_after_loop',
    bash_command='echo 1',
    dag=dag,
)
# [END howto_operator_bash]

run_this >> run_this_last

for i in range(3):
    task = BashOperator(
        task_id='runme_' + str(i),
        bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        dag=dag,
    )
    task >> run_this

# [START howto_operator_bash_template]
also_run_this = BashOperator(
  task_id='also_run_this',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)
# [END howto_operator_bash_template]
also_run_this >> run_this_last

if __name__ == "__main__":
    dag.cli()
