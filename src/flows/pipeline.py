import os

from prefect import flow, task
from prefect_shell import ShellOperation
from datetime import date, timedelta
from src.extractor.extract import extract_data
from src.transformer.transform import transform_data

# 1. Tarea para ejecutar el script de Bash
def run_shell_script(script_path: str):
    # La clase ShellOperation es una tarea predefinida
    ShellOperation(
        commands=[f"bash {script_path}"],
        stream_output=True,
        working_dir=os.getcwd(),
    ).run()

@task
def download_pdf(
    start_date: date = date.today(),
    end_date: date = date.today(),
):
    script_path = "./src/downloader/download_pdfs.sh"
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    command_with_args = f"{script_path} {start_date_str} {end_date_str}"
    print(f"Orquestando dbt a través de: {command_with_args}")
    
    # Se invoca la función
    run_shell_script(command_with_args)
    
    print("Flujo de orquestación completado.")
    pass

@task
def extract(
    start_date: date = date.today(),
    end_date: date = date.today(),
):
    extract_data()
    pass

@task
def transform(
    start_date: date = date.today(),
    end_date: date = date.today(),
):
    transform_data()
    pass

@task
def load(
    start_date: date = date.today(),
    end_date: date = date.today(),
):
    
    pass

@flow
def main():
    today = date.today()
    diff_time = timedelta(days=15)
    start_date = today - diff_time
    download_pdf(start_date)
    extract()


if __name__ == "__main__":
    main()