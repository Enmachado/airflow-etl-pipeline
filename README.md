from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def extrair_dados(**context):
    dados = {"valor": 100}
    print("📥 Extraindo dados...")
    print(f"Dados extraídos: {dados}")

    # Enviando dados via XCom
    context["ti"].xcom_push(key="dados_extraidos", value=dados)


def transformar_dados(**context):
    print("🔄 Transformando dados...")

    dados = context["ti"].xcom_pull(
        key="dados_extraidos",
        task_ids="extrair"
    )

    dados_transformados = {"valor": dados["valor"] * 2}

    print(f"Dados transformados: {dados_transformados}")

    context["ti"].xcom_push(key="dados_transformados", value=dados_transformados)


def carregar_dados(**context):
    print("📤 Carregando dados...")

    dados = context["ti"].xcom_pull(
        key="dados_transformados",
        task_ids="transformar"
    )

    print(f"Dados finais carregados: {dados}")


with DAG(
    dag_id="pipeline_etl_vscode",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["estudo", "etl"],
    description="Pipeline ETL simples para aprendizado",
) as dag:

    inicio = BashOperator(
        task_id="inicio_pipeline",
        bash_command="echo '🚀 Iniciando pipeline ETL'",
    )

    extrair = PythonOperator(
        task_id="extrair",
        python_callable=extrair_dados,
        provide_context=True,
    )

    transformar = PythonOperator(
        task_id="transformar",
        python_callable=transformar_dados,
        provide_context=True,
    )

    carregar = PythonOperator(
        task_id="carregar",
        python_callable=carregar_dados,
        provide_context=True,
    )

    fim = BashOperator(
        task_id="fim_pipeline",
        bash_command="echo '✅ Pipeline finalizado com sucesso!'",
    )

    # Ordem das tarefas
    inicio >> extrair >> transformar >> carregar >> fim
