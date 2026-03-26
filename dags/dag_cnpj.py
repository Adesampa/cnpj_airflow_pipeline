from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator

from cnpj_pipeline.transform_cnpj import run

TZ = pendulum.timezone("America/Sao_Paulo")


def is_last_monday(**context):
    data_execucao = context["data_interval_start"].in_timezone(TZ)
    proxima_semana = data_execucao + timedelta(days=7)

    # se 7 dias depois já mudou de mês, então esta é a última segunda
    return proxima_semana.month != data_execucao.month


with DAG(
    dag_id="cnpj_transform_mensal",
    start_date=datetime(2026, 1, 1, tzinfo=TZ),
    schedule="0 17 * * 1",   # toda segunda às 17h
    catchup=False,
    tags=["cnpj", "transform"],
) as dag:

    checar_ultima_segunda = ShortCircuitOperator(
        task_id="checar_ultima_segunda",
        python_callable=is_last_monday,
    )

    executar_transform = PythonOperator(
        task_id="executar_transform",
        python_callable=run,
    )

    fim = EmptyOperator(task_id="fim")

    checar_ultima_segunda >> executar_transform >> fim