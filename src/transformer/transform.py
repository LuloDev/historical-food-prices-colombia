# %% 
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

# %% Rutas
ROOT = Path(__file__).resolve().parent.parent.parent
bronze_dir = ROOT / "data" / "bronze"
silver_dir = ROOT / "data" / "silver"
silver_dir.mkdir(parents=True, exist_ok=True)

# %% Funciones auxiliares
def normalize_column_name(name: str) -> str:
    """Convierte nombres de columnas a formato limpio (snake_case, sin acentos)."""
    name = name.strip().lower()
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
        " ": "_",
        "-": "_",
        ".": "",
    }
    for k, v in replacements.items():
        name = name.replace(k, v)
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)
    return name


def to_numeric_safe(series: pd.Series) -> pd.Series:
    """Convierte una serie a tipo numérico, forzando errores a NaN."""
    return pd.to_numeric(series, errors="coerce")


def consolidate_dataframes(dfs: List[pd.DataFrame], output_name: str):
    """Consolida una lista de DataFrames en un único archivo Parquet."""
    if not dfs:
        print("🟡 No hay datos para consolidar.")
        return

    print(f"\n📦 Consolidando {len(dfs)} dataframes en {output_name}...")
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.convert_dtypes()
    combined_path = silver_dir / f"{output_name}.parquet"
    combined_df.to_parquet(combined_path, index=False)
    print(f"💾 Consolidado guardado en: {combined_path} ({len(combined_df)} filas)")


# %% Lógica de Transformación


def transform_single_csv(csv_file: Path) -> Optional[pd.DataFrame]:
    """
    Transforma los datos de un único archivo CSV y devuelve un DataFrame.
    Devuelve None si el archivo no se puede procesar.
    """
    try:
        csv_file = Path(csv_file)
        print(f"📄 Transformando: {csv_file.name}")
        df = pd.read_csv(csv_file)
        df.columns = [normalize_column_name(c) for c in df.columns]

        # Detectar versión por las columnas existentes
        if "precio_calidad_extra" in df.columns:
            version = "v1"
        elif "precio_extra" in df.columns:
            version = "v2"
        else:
            print(f"⚠️ No se detecta formato en {csv_file.name}, se omite.")
            return None

        # Normalización de columnas según la versión del PDF
        if version == "v1":
            df = df.rename(
                columns={
                    "precio_calidad_extra": "precio_extra",
                    "precio_calidad_primera": "precio_primera",
                    "valor_x_kilo": "precio_unidad",
                }
            )
            df["variacion"] = None  # La v1 no tiene esta columna

        # Estandarizar tipos de datos
        df["cantidad"] = to_numeric_safe(df["cantidad"])
        for col in ["precio_extra", "precio_primera", "precio_unidad"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r"[^\d.]", "", regex=True)
                    .replace("", None)
                )
                df[col] = to_numeric_safe(df[col])

        # Asegurar que todas las columnas esperadas existan
        expected_cols = [
            "producto",
            "presentacion",
            "cantidad",
            "unidad",
            "precio_extra",
            "precio_primera",
            "precio_unidad",
            "variacion",
        ]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None

        df = df[expected_cols]
        df["source_file"] = csv_file.stem
        df = df.convert_dtypes()

        # Guardar el archivo individual en Parquet
        parquet_path = silver_dir / (csv_file.stem + ".parquet")
        df.to_parquet(parquet_path, index=False)
        print(f"💾 Guardado: {parquet_path} (formato {version})")

        return df

    except Exception as e:
        print(f"⚠️ Error transformando {csv_file.name}: {e}")
        return None


def transform_data(csv_files: Optional[List[Path]] = None):
    """
    Transforma una lista de archivos CSV y los consolida en un único Parquet.
    """
    if csv_files is None:
        print("ℹ️ No se proporcionaron archivos. Buscando todos los CSV en data/bronze/...")
        csv_files = sorted(bronze_dir.glob("*.csv"))

    if not csv_files:
        print("⚠️ No hay archivos CSV para transformar.")
        return

    all_dfs = []
    for csv_file in csv_files:
        df = transform_single_csv(csv_file)
        if df is not None:
            all_dfs.append(df)

    consolidate_dataframes(all_dfs, "all_data")
    print("\n✅ Transformación completada con éxito.")


if __name__ == "__main__":
    transform_data()