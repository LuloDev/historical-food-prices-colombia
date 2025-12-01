import os
import re
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd
from prefect import flow, task
from prefect_shell import ShellOperation

from src.extractor.extract import process_single_pdf
from src.transformer.transform import consolidate_dataframes, transform_single_csv


@task(retries=3, retry_delay_seconds=60, name="Descargar PDFs de Precios")
def download_pdfs_task(start_date: date, end_date: date) -> List[str]:
    """
    Ejecuta el script de descarga, filtra la salida para obtener solo rutas de
    archivos válidas y existentes, y las devuelve.
    """
    script_path = "./src/downloader/download_pdfs.sh"
    command = f"bash {script_path} {start_date:%Y-%m-%d} {end_date:%Y-%m-%d}"

    print(f"🚀 Ejecutando comando de descarga: {command}")
    result = ShellOperation(commands=[command], stream_output=True).run()

    clean_paths = []
    for line in result:
        match = re.search(r"(/[\w/.-]+\.pdf)", line)
        if match:
            path = match.group(1)
            if os.path.exists(path):
                clean_paths.append(path)

    print(f"✅ Se encontraron {len(clean_paths)} rutas de PDF válidas.")
    return clean_paths


@task(name="Extraer Datos de PDF")
def extract_one_pdf_task(pdf_file: str) -> Optional[Path]:
    """
    Task para extraer datos de un único archivo PDF.
    """
    return process_single_pdf(Path(pdf_file))


@task(name="Transformar CSV a DataFrame")
def transform_one_csv_task(csv_file: Path) -> Optional[pd.DataFrame]:
    """
    Task para transformar un único archivo CSV a un DataFrame de pandas.
    """
    return transform_single_csv(csv_file)


@task(name="Filtrar Resultados Nulos")
def filter_none_results(items: List) -> List:
    """
    Filtra los elementos 'None' de una lista de resultados de tasks.
    """
    filtered_items = [item for item in items if item is not None]
    print(f"ℹ️ Se filtraron {len(items) - len(filtered_items)} resultados nulos.")
    return filtered_items


@task(name="Consolidar Datos")
def consolidate_task(dataframes: List[pd.DataFrame]):
    """
    Task para consolidar una lista de DataFrames en un único archivo Parquet.
    """
    consolidate_dataframes(dataframes, "daily_prices")


@task(name="Cargar Datos (Placeholder)")
def load_task():
    """Placeholder para la tarea de carga de datos a un destino final."""
    print("🚀 Tarea de carga (load) iniciada. Actualmente es un placeholder.")
    print("✅ Tarea de carga finalizada.")
    pass


@flow(name="ETL Principal - Precios de Alimentos Corabastos")
def etl_pipeline(start_date: date, end_date: date):
    """
    Flujo ETL principal para descargar, extraer, transformar y cargar los precios de alimentos.
    Las etapas de extracción y transformación se ejecutan en paralelo.
    """
    print(f"🏁 Iniciando pipeline ETL para el rango de fechas: {start_date} a {end_date}")

    pdf_files = download_pdfs_task(start_date=start_date, end_date=end_date)

    if not pdf_files:
        print("🟡 La descarga no produjo archivos, el pipeline finaliza.")
        return

    # Etapa de Extracción en Paralelo
    csv_path_futures = extract_one_pdf_task.map(pdf_files)
    valid_csv_paths = filter_none_results(csv_path_futures)

    if not valid_csv_paths:
        print("🟡 La extracción no produjo archivos CSV válidos, el pipeline finaliza.")
        return

    # Etapa de Transformación en Paralelo
    dataframe_futures = transform_one_csv_task.map(valid_csv_paths)
    valid_dataframes = filter_none_results(dataframe_futures)

    if not valid_dataframes:
        print("🟡 La transformación no produjo DataFrames válidos, el pipeline finaliza.")
        return

    # Etapa de Consolidación y Carga
    consolidation_future = consolidate_task.submit(valid_dataframes)
    load_task(wait_for=[consolidation_future])

    print("🏁 Pipeline ETL finalizado con éxito.")


if __name__ == "__main__":
    end_date_run = date.today()
    start_date_run = end_date_run - timedelta(weeks=2)

    print(f"Ejecutando el pipeline localmente para el rango: {start_date_run} a {end_date_run}")
    etl_pipeline(start_date=start_date_run, end_date=end_date_run)
