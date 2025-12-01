# %%
import pandas as pd
import re
from pathlib import Path

# %% Rutas
ROOT = Path(__file__).resolve().parent.parent.parent
pdf_dir = ROOT / "data" / "raw_pdfs"
bronze_dir = ROOT / "data" / "bronze"
bronze_dir.mkdir(parents=True, exist_ok=True)
silver_dir = ROOT / "data" / "silver"
silver_dir.mkdir(parents=True, exist_ok=True)

# %% Funciones auxiliares
def normalize_column_name(name: str) -> str:
    """Convierte nombres de columnas a formato limpio (snake_case, sin acentos)."""
    replacements = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n",
        " ": "_", "-": "_", ".": ""
    }
    for k, v in replacements.items():
        name = name.replace(k, v)
    name = re.sub(r"[^a-zA-Z0-9_]", "", name)
    return name.lower()

def to_numeric_safe(series: pd.Series) -> pd.Series:
    """Convierte valores a float si es posible."""
    return pd.to_numeric(series, errors="coerce")

def consolidate(pdfs):
    if pdfs:
        combined_df = pd.concat(pdfs, ignore_index=True)
        combined_df = combined_df.convert_dtypes()
        combined_path = silver_dir / "all_data.parquet"
        combined_df.to_parquet(combined_path, index=False)
        print(f"\n📦 Consolidado guardado en: {combined_path} ({len(combined_df)} filas)")

# %% Transformación unificada
def transform_data():
    csv_files = sorted(bronze_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("❌ No hay archivos CSV en bronze/")
    all_dfs = []

    for csv_file in csv_files:
        print(f"\n📄 Transformando: {csv_file.name}")
        df = pd.read_csv(csv_file)
        df.columns = [normalize_column_name(c) for c in df.columns]

        # Detectar versión
        if "precio_calidad_extra" in df.columns:
            version = "v1"
        elif "precio_extra" in df.columns:
            version = "v2"
        else:
            print(f"⚠️ No se detecta formato en {csv_file.name}, se omite.")
            continue

        # Normalización por versión
        if version == "v1":
            df = df.rename(columns={
                "precio_calidad_extra": "precio_extra",
                "precio_calidad_primera": "precio_primera",
                "valor_x_kilo": "precio_unidad"
            })
            if "cantidad" not in df.columns:
                df["cantidad"] = None
            df["variacion"] = None

        elif version == "v2":
            for col in ["cantidad", "precio_extra", "precio_primera", "precio_unidad", "variacion"]:
                if col not in df.columns:
                    df[col] = None

        # Estandarizar tipos
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce")
        for col in ["precio_extra", "precio_primera", "precio_unidad"]:
            # Remover signos de $ o comas antes de convertir
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[\$,]", "", regex=True)
                .replace("None", None)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Columnas finales
        expected_cols = [
            "producto", "presentacion", "cantidad", "unidad",
            "precio_extra", "precio_primera", "precio_unidad", "variacion"
        ]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None

        df = df[expected_cols]
        df["source_file"] = csv_file.stem

        # Convertir todo a tipos uniformes antes de guardar
        df = df.convert_dtypes()

        all_dfs.append(df)

        parquet_path = silver_dir / (csv_file.stem + ".parquet")
        df.to_parquet(parquet_path, index=False)
        consolidate(all_dfs)
        print(f"💾 Guardado: {parquet_path} ({len(df)} filas, formato {version})")
        print("\n✅ Transformación completada con éxito.")

if __name__ == "__main__":
    transform_data()