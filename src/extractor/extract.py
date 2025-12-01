# %%
import pdfplumber
import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# %% Configuración de rutas

ROOT = Path(__file__).resolve().parent.parent.parent
pdf_dir = ROOT / "data" / "raw_pdfs"
bronze_dir = ROOT / "data" / "bronze"
bronze_dir.mkdir(parents=True, exist_ok=True)
    # Fecha límite de cambio de formato
CHANGE_DATE = datetime(2025, 2, 25)

def parse_pdf_date(pdf_file: Path) -> datetime:
    """Extrae la fecha del nombre del archivo (ej: 2024-05-23.pdf)."""
    match = re.search(r"(\d{4}-\d{2}-\d{2})", pdf_file.stem)
    if not match:
        raise ValueError(f"No se encontró fecha en el nombre: {pdf_file.name}")
    return datetime.strptime(match.group(1), "%Y-%m-%d")

# %% Funciones de extracción (v1 y v2)

def extract_v1(pdf_file: Path) -> pd.DataFrame:
    """Versión 1: PDFs antes del 25 de febrero de 2025"""
    rows = []
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            lines = [re.sub(r'\s+', ' ', l.strip()) for l in text.split('\n') if l.strip()]
            for line in lines:
                if re.search(r'(nombre|presentación|unidad|valor|cal\.|análisis|analisis)', line, re.IGNORECASE):
                    continue
                if not re.search(r'\$\s?\d', line):
                    continue

                precios = re.findall(r'\$\s?\d{1,3}(?:\.\d{3})*(?:,\d{2})?', line)
                if not precios:
                    continue

                precio_extra = precios[0] if len(precios) >= 1 else None
                precio_primera = precios[1] if len(precios) >= 2 else None
                valor_kilo = precios[2] if len(precios) >= 3 else None

                sin_precios = re.sub(r'\$\s?\d{1,3}(?:\.\d{3})*(?:,\d{2})?', '', line).strip()
                parts = sin_precios.split(" ")

                producto = " ".join(parts[:-3]) if len(parts) > 3 else sin_precios
                presentacion = parts[-3] if len(parts) >= 3 else None
                cantidad = parts[-2] if len(parts) >= 2 else None
                unidad = parts[-1] if len(parts) >= 1 else None

                rows.append({
                    "producto": producto.strip(),
                    "presentacion": presentacion,
                    "cantidad": cantidad,
                    "unidad": unidad,
                    "precio_calidad_extra": precio_extra,
                    "precio_calidad_primera": precio_primera,
                    "valor_x_kilo": valor_kilo,
                })

    df = pd.DataFrame(rows)
    for col in ["precio_calidad_extra", "precio_calidad_primera", "valor_x_kilo"]:
        df[col] = (
            df[col]
            .str.replace(r"[^\d,]", "", regex=True)
            .str.replace(",", "")
            .replace("", None)
            .astype(float)
        )
    return df

def extract_v2(pdf_file: Path) -> pd.DataFrame:
    """Versión mejorada: PDFs después del 25 de febrero de 2025"""
    rows = []

    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            # Limpieza básica de texto
            lines = [re.sub(r'\s+', ' ', line.strip()) for line in text.split('\n') if line.strip()]

            for line in lines:
                # Saltar encabezados y líneas vacías
                if re.search(r'precio|unidad|calidad|nombre', line, re.IGNORECASE):
                    continue

                # Detectar líneas con precios válidos
                if not re.search(r'\$\s?\d|(?:\d{1,3}(?:[\.,]\d{3})*[,\.]?\d*)', line):
                    continue

                # Separar partes
                parts = line.split()

                # Buscar índices donde aparecen los precios
                price_indices = [i for i, p in enumerate(parts) if re.search(r'\$\s?\d|^\d{3,}$', p.replace(",", "").replace(".", ""))]
                if len(price_indices) < 2:
                    continue

                # Buscar la palabra que indica tipo de presentación (BULTO, ATADO, CANASTILLA, etc.)
                match_pres = next((i for i, p in enumerate(parts) if p.upper() in ["BULTO", "ATADO", "CANASTILLA", "BOLSA", "KILO", "CAJA", "LIBRA"]), None)
                if match_pres is None or match_pres + 2 >= len(parts):
                    continue

                producto = " ".join(parts[:match_pres])
                presentacion = parts[match_pres]
                cantidad = parts[match_pres + 1]
                unidad = parts[match_pres + 2]

                # Extraer precios (3 columnas numéricas seguidas)
                precios_raw = [p for p in parts[match_pres + 3:] if re.search(r'\d', p)]
                precios = [re.sub(r'[^\d,\.]', '', p) for p in precios_raw[:3]]

                # Último valor no numérico = variación
                variacion = next((p for p in reversed(parts) if not re.search(r'\d', p)), None)

                if len(precios) >= 3:
                    row = {
                        "producto": producto.strip(),
                        "presentacion": presentacion,
                        "cantidad": cantidad,
                        "unidad": unidad,
                        "precio_extra": precios[0],
                        "precio_primera": precios[1],
                        "precio_unidad": precios[2],
                        "variacion": variacion,
                    }
                    rows.append(row)

    df = pd.DataFrame(rows)

    # Filtrar filas inválidas (producto vacío, sin precios, etc.)
    df = df[df["producto"].str.len() > 2]
    df = df[df["precio_extra"].notna()]

    return df


# %% Procesar todos los PDFs según su fecha

def extract_data():
    pdf_files = sorted(pdf_dir.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError("❌ No hay PDFs en raw_pdfs/")

    for pdf_file in pdf_files:
        pdf_date = parse_pdf_date(pdf_file)
        extractor = extract_v1 if pdf_date < CHANGE_DATE else extract_v2
        version = "v1 (antes del 25-feb-2025)" if extractor == extract_v1 else "v2 (después del 25-feb-2025)"
        print(f"\n📄 Procesando: {pdf_file.name} → usando {version}")

        try:
            df = extractor(pdf_file)
            csv_path = bronze_dir / (pdf_file.stem + ".csv")
            df.to_csv(csv_path, index=False)
            print(f"💾 Guardado: {csv_path}")
        except Exception as e:
            print(f"⚠️ Error procesando {pdf_file.name}: {e}")

    print("\n✅ Procesamiento completado.")

if __name__ == "__main__":
    extract_data()