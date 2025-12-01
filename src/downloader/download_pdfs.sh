#!/usr/bin/env bash

# --- Configuración ---
# Uso: ./descargar_boletines.sh 2025-02-01 2025-03-10
# Descarga los boletines diarios de Corabastos dentro del rango de fechas.
# Imprime las rutas absolutas de los archivos que existen o que fueron
# descargados exitosamente. Redirige logs a stderr.

START_DATE=$1
END_DATE=$2
OUTPUT_DIR="data/raw_pdfs"

mkdir -p "$OUTPUT_DIR"

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
  echo "Uso: $0 <fecha_inicio: YYYY-MM-DD> <fecha_fin: YYYY-MM-DD>" >&2
  exit 1
fi

meses=("enero" "febrero" "marzo" "abril" "mayo" "junio" "julio" "agosto" "septiembre" "octubre" "noviembre" "diciembre")

start_ts=$(date -d "$START_DATE" +%s)
end_ts=$(date -d "$END_DATE" +%s)
cutoff_ts=$(date -d "2025-02-25" +%s)

current_ts=$start_ts
while [ "$current_ts" -le "$end_ts" ]; do
  current_date=$(date -d "@$current_ts" +%Y-%m-%d)
  year=$(date -d "@$current_ts" +%Y)
  month=$(date -d "@$current_ts" +%m)
  day=$(date -d "@$current_ts" +%d)
  day_no_zero=$(date -d "@$current_ts" +%-d)

  month_index=$((10#$month - 1))
  month_name=${meses[$month_index]}

  output_file_relative="${OUTPUT_DIR}/${current_date}.pdf"
  output_file_abs=$(readlink -f "$output_file_relative")

  # Opción 1: El archivo ya existe localmente.
  if [ -f "$output_file_abs" ]; then
    echo "⏩ Ya existe: $output_file_abs (usando)" >&2
    echo "$output_file_abs" # Imprimir ruta a stdout
  # Opción 2: El archivo no existe, intentar descargarlo.
  else
    if [ "$current_ts" -lt "$cutoff_ts" ]; then
      url="https://corabastos.com.co/wp-content/uploads/${year}/${month}/Boletin-${day_no_zero}${month_name}${year}.pdf"
    else
      url="https://corabastos.com.co/wp-content/uploads/${year}/${month}/Boletin_diario_${year}${month}${day}.pdf"
    fi

    echo "⬇️  Descargando: $url → $output_file_abs" >&2
    
    # Usar curl con -f para que devuelva un error si el HTTP status es de fallo (ej. 404)
    curl -s -f -L "$url" -o "$output_file_abs"
    
    # Si la descarga fue exitosa (exit code 0), imprimir la ruta.
    if [ $? -eq 0 ]; then
      echo "✅ Guardado: $output_file_abs" >&2
      echo "$output_file_abs" # Imprimir ruta a stdout
    # Si la descarga falló, limpiar y no imprimir nada a stdout.
    else
      echo "⚠️  No disponible: $url" >&2
      rm -f "$output_file_abs"
    fi
  fi

  current_ts=$((current_ts + 86400))
done
