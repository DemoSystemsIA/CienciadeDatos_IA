import streamlit as st
import pandas as pd
from io import BytesIO
import os

# --------------------------------------------------
# CONFIGURACIÓN
# --------------------------------------------------
st.set_page_config(
    page_title="Desglose por TC con decimales",
    layout="wide"
)

st.title("📊 Desglose por TC según número de decimales")

# --------------------------------------------------
# CARGA DE ARCHIVO
# --------------------------------------------------
uploaded_file = st.file_uploader(
    "📂 Sube el archivo Excel",
    type=["xlsx"]
)

if not uploaded_file:
    st.stop()

nombre_base = os.path.splitext(uploaded_file.name)[0]

# --------------------------------------------------
# LECTURA DE EXCEL
# --------------------------------------------------
try:
    df = pd.read_excel(uploaded_file)
    df.columns = df.columns.str.strip()
except Exception as e:
    st.error("❌ No se pudo leer el archivo Excel")
    st.stop()

st.success(f"✅ Archivo cargado correctamente ({len(df):,} registros)")

# --------------------------------------------------
# VALIDACIÓN DE COLUMNAS
# --------------------------------------------------
columnas_requeridas = [
    "Item",
    "Descripción",
    "Unidad",
    "Cantidad",
    "Precio",
    "%DR",
    "Subtotal",
    "Lote",
    "Fecha Vcto",
    "Centro Costo",
    "Desc. Centro Costo",
    "Bodega",
    "Descripción Bodega",
    "Observación",
    "TC"
]

faltantes = [c for c in columnas_requeridas if c not in df.columns]

if faltantes:
    st.error(f"❌ Faltan columnas obligatorias: {', '.join(faltantes)}")
    st.stop()

# Mantener solo columnas necesarias (en orden)
df = df[columnas_requeridas].copy()

# --------------------------------------------------
# SELECTOR DE DECIMALES
# --------------------------------------------------
decimales = st.slider(
    "🔢 Selecciona cantidad de decimales para agrupar TC",
    min_value=0,
    max_value=6,
    value=2,
    step=1
)

# --------------------------------------------------
# COLUMNA TC LÓGICA (REDONDEADA)
# --------------------------------------------------
df["TC_grupo"] = df["TC"].round(decimales)

# --------------------------------------------------
# RESUMEN DE AGRUPACIÓN
# --------------------------------------------------
resumen = (
    df.groupby("TC_grupo")
      .size()
      .reset_index(name="Registros")
      .sort_values("TC_grupo")
)

st.subheader("📋 Resumen de hojas a generar")
st.dataframe(resumen, use_container_width=True)

st.info(f"📁 Se generarán {len(resumen)} hojas en el Excel")

# --------------------------------------------------
# VISUALIZACIÓN DE GRUPOS
# --------------------------------------------------
st.subheader("🔎 Visualizar registros por TC")

tc_seleccionado = st.selectbox(
    "Selecciona un TC",
    resumen["TC_grupo"].tolist()
)

df_filtrado = df[df["TC_grupo"] == tc_seleccionado]

st.caption(f"Mostrando registros para TC = {tc_seleccionado:.{decimales}f}")

st.dataframe(
    df_filtrado.drop(columns=["TC_grupo"]),
    use_container_width=True
)

# --------------------------------------------------
# EXPORTAR EXCEL AGRUPADO
# --------------------------------------------------
output = BytesIO()

with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
    for tc_val, grupo in df.groupby("TC_grupo"):
        nombre_hoja = f"TC_{tc_val:.{decimales}f}".replace(".", "_")[:31]
        grupo.drop(columns=["TC_grupo"]).to_excel(
            writer,
            sheet_name=nombre_hoja,
            index=False
        )

output.seek(0)

nombre_salida = f"{nombre_base}_TC_{decimales}_decimales.xlsx"

st.download_button(
    "📥 Descargar Excel desglosado por TC",
    data=output,
    file_name=nombre_salida,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
