from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *", # 분 시 일 월 요일, 매일 0시 0분에 실행
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"), # 시작하는 날짜
    catchup=False, # start_date가 현재 날짜 기준으로 과거인 경우 start_date부터 현재 날짜까지의 워크플로우를 실행하는지 여부
    dagrun_timeout=datetime.timedelta(minutes=60), # 타임아웃, 몇 분 동안 이상 돌게 되면 실패되도록 설정하는 기능
    tags=["example", "example2"], # 태그
    params={"example_key": "example_value"}, # 태스크들의 공통 파라미터
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last", # 태스크 이름
    )

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="ls -alh --color=always / && echo https://airflow.apache.org/  && echo 'some <code>html</code>'", # 실행하는 bash 명령
    )
    # [END howto_operator_bash]

    run_this >> run_this_last

    for i in range(3):
        task = BashOperator(
            task_id=f"runme_{i}",
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    # [START howto_operator_bash_template]
    also_run_this = BashOperator(
        task_id="also_run_this",
        bash_command='echo "ti_key={{ task_instance_key_str }}"',
    )
    # [END howto_operator_bash_template]
    also_run_this >> run_this_last

# [START howto_operator_bash_skip]
this_will_skip = BashOperator(
    task_id="this_will_skip",
    bash_command='echo "hello world"; exit 99;',
    dag=dag,
)
# [END howto_operator_bash_skip]
this_will_skip >> run_this_last

if __name__ == "__main__":
    dag.test()