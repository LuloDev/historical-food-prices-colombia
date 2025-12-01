#!/usr/bin/env bash

# --- Configuración ---
# Uso: ./descargar_boletines.sh 2025-02-01 2025-03-10
# Descarga los boletines diarios de Corabastos dentro del rango de fechas.
# Usa el formato antiguo antes del 25 de febrero de 2025 (con mes en español).
# Si el archivo ya existe, lo omite.

START_DATE=$1
END_DATE=$2
OUTPUT_DIR="data/raw_pdfs"

mkdir -p "$OUTPUT_DIR"

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
  echo "Uso: $0 <fecha_inicio: YYYY-MM-DD> <fecha_fin: YYYY-MM-DD>"
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

  month_index=$((10#$month - 1)) # 10# evita problemas con ceros a la izquierda
  month_name=${meses[$month_index]}

  output_file="${OUTPUT_DIR}/${current_date}.pdf"

  if [ -f "$output_file" ]; then
    echo "⏩ Ya existe: $output_file (omitido)"
  else
    if [ "$current_ts" -lt "$cutoff_ts" ]; then
      url="https://corabastos.com.co/wp-content/uploads/${year}/${month}/Boletin-${day_no_zero}${month_name}${year}.pdf"
    else
      url="https://corabastos.com.co/wp-content/uploads/${year}/${month}/Boletin_diario_${year}${month}${day}.pdf"
    fi

    echo "⬇️  Descargando: $url → $output_file"

    curl -s -f -L "$url" -o "$output_file"
    if [ $? -eq 0 ]; then
      echo "✅ Guardado: $output_file"
    else
      echo "⚠️  No disponible: $url"
      rm -f "$output_file"
    fi
  fi

  current_ts=$((current_ts + 86400))
done
