
import streamlit as st
import pandas as pd
import psycopg2
from io import BytesIO
from datetime import datetime

st.set_page_config(page_title="Distribución de horas según porcentajes Packing-Maquila (ZUPRA)", layout="wide")


# ---------------- Conexión a PostgreSQL ----------------
def get_postgres_data():
    """Conecta a Postgres usando st.secrets y devuelve DataFrame.
    Ajusta según tu entorno si no usas st.secrets.
    """
    conn = psycopg2.connect(
        host=st.secrets["postgres"]["host"],
        dbname=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"]
    )
    query = """
    SELECT *
    FROM raw.pe_ccoz_distribuciongth;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ---------------- Helpers ----------------

def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


def ensure_date(x):
    try:
        return pd.to_datetime(x, errors="coerce").date()
    except:
        return None


# ---------------- Interfaz ----------------
st.title("📊 Distribución de horas según porcentajes de kilos ZUPRA")
uploaded_file = st.file_uploader("Sube la estructura correcta en excel", type=["xlsx"]) 

if uploaded_file:
    # ---------------- Leer hojas ----------------
    xls = pd.read_excel(uploaded_file, sheet_name=None)
    # Normalizamos nombres de hojas a mayúsculas sin espacios alrededor
    sheets = {k.strip().upper(): v for k, v in xls.items()}

    # Buscar hoja por nombres posibles (tolerante a variantes)
    def get_sheet_by_name(possible_names):
        for name in possible_names:
            if name.strip().upper() in sheets:
                return sheets[name.strip().upper()].copy()
        return pd.DataFrame()

    df_tareo = get_sheet_by_name(["TAREO PACKING", "TAREO_PACKING", "TAREO"])
    df_dni = get_sheet_by_name(["DNI"])
    df_labores = get_sheet_by_name(["LABORES", "LABOR", "ACTIVIDADES"])    

    # Si alguna hoja está vacía, creamos df vacío con columnas mínimas para evitar errores posteriores
    if df_tareo is None or df_tareo.empty:
        df_tareo = pd.DataFrame()
    if df_dni is None or df_dni.empty:
        df_dni = pd.DataFrame()
    if df_labores is None or df_labores.empty:
        df_labores = pd.DataFrame()

    # Limpiar nombres columnas (strip)
    for df in [df_tareo, df_dni, df_labores]:
        if not df.empty:
            df.columns = [str(c).strip() for c in df.columns]

    # ---------------- Normalización TAREO ----------------
    # Aseguramos columna DNI en tareo con nombre estándar "N° DNI"
    if "N° DNI" in df_tareo.columns:
        df_tareo["N° DNI"] = df_tareo["N° DNI"].astype(str).str.strip()
    elif "N°DNI" in df_tareo.columns:
        df_tareo["N° DNI"] = df_tareo["N°DNI"].astype(str).str.strip()
    else:
        possible = [c for c in df_tareo.columns if "DNI" in c.upper()]
        if possible:
            df_tareo["N° DNI"] = df_tareo[possible[0]].astype(str).str.strip()
        else:
            df_tareo["N° DNI"] = ""

    # Campos FECHA
    if "FECHA" in df_tareo.columns:
        df_tareo["FECHA"] = pd.to_datetime(df_tareo["FECHA"], errors="coerce").dt.date
    elif "F. INGRESO" in df_tareo.columns:
        df_tareo["FECHA"] = pd.to_datetime(df_tareo["F. INGRESO"], errors="coerce").dt.date
    else:
        # intentar encontrar alguna columna fecha
        possible_fecha = [c for c in df_tareo.columns if "FECHA" in c.upper()]
        if possible_fecha:
            df_tareo["FECHA"] = pd.to_datetime(df_tareo[possible_fecha[0]], errors="coerce").dt.date
        else:
            df_tareo["FECHA"] = pd.NaT

    # Aseguramos columnas que usaremos y convertimos a string cuando corresponda
    def ensure_cols_exist(df, cols):
        for col in cols:
            if col not in df.columns:
                df[col] = ""

    ensure_cols_exist(df_tareo, ["AREA","GRUPO","COD","SEM","CODIGO","DESCRIPCION DE LABOR","CECO","HE_D","H_NOCTURNAS","APELLIDOS Y NOMBRES"])

    # Convertir a string columnas relevantes
    for c in ["AREA","CECO","CODIGO"]:
        if c in df_tareo.columns:
            df_tareo[c] = df_tareo[c].astype(str).str.strip()

    # ---------------- Normalización DNI ----------------
    if "DNI" in df_dni.columns:
        df_dni["DNI"] = df_dni["DNI"].astype(str).str.strip()
    else:
        posible = [c for c in df_dni.columns if "DNI" in c.upper()]
        if posible:
            df_dni["DNI"] = df_dni[posible[0]].astype(str).str.strip()
        else:
            df_dni["DNI"] = ""

    # FECHA_INGRESO
    if "FECHA_INGRESO" in df_dni.columns:
        df_dni["FECHA_INGRESO"] = pd.to_datetime(df_dni["FECHA_INGRESO"], errors="coerce").dt.date
    else:
        possible_fecha = [c for c in df_dni.columns if "FECHA" in c.upper() and "ING" in c.upper()]
        if possible_fecha:
            df_dni["FECHA_INGRESO"] = pd.to_datetime(df_dni[possible_fecha[0]], errors="coerce").dt.date
        else:
            df_dni["FECHA_INGRESO"] = pd.NaT

    # APELLIDOS
    if "APELLIDOS" in df_dni.columns:
        df_dni["APELLIDOS"] = df_dni["APELLIDOS"].astype(str).str.strip()
    else:
        posible = [c for c in df_dni.columns if "APELL" in c.upper() or "NOMBRE" in c.upper()]
        if posible:
            df_dni["APELLIDOS"] = df_dni[posible[0]].astype(str).str.strip()
        else:
            df_dni["APELLIDOS"] = ""

    # ---------------- Normalización LABORES ----------------
    if "CODIGO" in df_labores.columns:
        df_labores["CODIGO"] = df_labores["CODIGO"].astype(str).str.strip()
    else:
        possible = [c for c in df_labores.columns if "COD" in c.upper() and "LAB" not in c.upper()]
        if possible:
            df_labores["CODIGO"] = df_labores[possible[0]].astype(str).str.strip()
        else:
            df_labores["CODIGO"] = ""

    possible_lab = [c for c in df_labores.columns if "LAB" in c.upper() and "COD" not in c.upper()]
    if possible_lab:
        df_labores["Labor"] = df_labores[possible_lab[0]].astype(str).str.strip()
    else:
        posible_desc = [c for c in df_labores.columns if "DESCRIP" in c.upper() or "NOMBRE" in c.upper()]
        if posible_desc:
            df_labores["Labor"] = df_labores[posible_desc[0]].astype(str).str.strip()
        else:
            df_labores["Labor"] = ""

    # ID_ACTIVIDAD
    possible_id = [c for c in df_labores.columns if "ID" in c.upper() and "ACT" in c.upper()]
    if possible_id:
        df_labores["ID_ACTIVIDAD"] = df_labores[possible_id[0]].astype(str).str.strip()
    else:
        df_labores["ID_ACTIVIDAD"] = df_labores.get("ID-ACT", "").astype(str).str.strip()

    # COD_LABOR
    possible_c_lab = [c for c in df_labores.columns if "COD_LAB" in c.upper() or "COD_L" in c.upper()]
    if possible_c_lab:
        df_labores["COD_LABOR"] = df_labores[possible_c_lab[0]].astype(str).str.strip()
    else:
        df_labores["COD_LABOR"] = df_labores.get("C_LAB", "").astype(str).str.strip()

    # ---------------- Asegurar columnas packing / maquila en df_postgres mapping ----------------
    df_postgres = get_postgres_data()
    # Normalizar df_postgres cuando existen columnas con distintos nombres
    if "fecha" in df_postgres.columns:
        df_postgres["fecha"] = pd.to_datetime(df_postgres["fecha"], errors="coerce").dt.date
    else:
        possible = [c for c in df_postgres.columns if "FECHA" in c.upper()]
        if possible:
            df_postgres["fecha"] = pd.to_datetime(df_postgres[possible[0]], errors="coerce").dt.date
        else:
            df_postgres["fecha"] = pd.NaT

    if "area" in df_postgres.columns:
        df_postgres["area"] = df_postgres["area"].astype(str).str.strip().str.upper()
    else:
        possible = [c for c in df_postgres.columns if "AREA" in c.upper()]
        if possible:
            df_postgres["area"] = df_postgres[possible[0]].astype(str).str.strip().str.upper()
        else:
            df_postgres["area"] = ""

    # packing
    if "packing" not in df_postgres.columns:
        for c in df_postgres.columns:
            if "PACK" in c.upper():
                df_postgres["packing"] = pd.to_numeric(df_postgres[c], errors="coerce").fillna(0)
                break
        else:
            df_postgres["packing"] = 0
    else:
        df_postgres["packing"] = pd.to_numeric(df_postgres["packing"], errors="coerce").fillna(0)

    # servicio maquila
    if "SERVICIO MAQUILA" not in df_postgres.columns and "servicio_maquila" in df_postgres.columns:
        df_postgres["SERVICIO MAQUILA"] = pd.to_numeric(df_postgres["servicio_maquila"], errors="coerce").fillna(0)
    elif "SERVICIO MAQUILA" not in df_postgres.columns:
        for c in df_postgres.columns:
            if "MAQUILA" in c.upper():
                df_postgres["SERVICIO MAQUILA"] = pd.to_numeric(df_postgres[c], errors="coerce").fillna(0)
                break
        else:
            df_postgres["SERVICIO MAQUILA"] = 0
    else:
        df_postgres["SERVICIO MAQUILA"] = pd.to_numeric(df_postgres["SERVICIO MAQUILA"], errors="coerce").fillna(0)

    # ---------------- Preparar TAREO para merges ----------------
    # CECO/AREA
    if "AREA" in df_tareo.columns:
        df_tareo["AREA"] = df_tareo["AREA"].astype(str).str.strip()
    else:
        df_tareo["AREA"] = ""

    if "CECO" in df_tareo.columns:
        df_tareo["CECO"] = df_tareo["CECO"].astype(str).str.strip()
    else:
        df_tareo["CECO"] = "Sin CECO"

    # Crear AREA2_tmp igual que script original (mapear AREA)
    def map_area(area):
        a = str(area).strip().upper()
        if a in ["OBRAS EN CURSO", "GESTION DEL TALENTO HUMANO", "SSOMA"]:
            return "NO"
        elif a in ["PRODUCCION", "ALMACEN DE PISO PRODUCCION"]:
            return "PRODUCCION"
        else:
            return "RECEPCION"

    df_tareo["AREA2_tmp"] = df_tareo["AREA"].apply(map_area)

    # ---------------- Merge TAREO con POSTGRES ----------------
    df_tareo_for_merge = df_tareo.copy()
    # aseguramos FECHA en df_tareo_for_merge
    if "FECHA" not in df_tareo_for_merge.columns or df_tareo_for_merge["FECHA"].isnull().all():
        possible_fecha = [c for c in df_tareo_for_merge.columns if "FECHA" in c.upper()]
        if possible_fecha:
            df_tareo_for_merge["FECHA"] = pd.to_datetime(df_tareo_for_merge[possible_fecha[0]], errors="coerce").dt.date
        else:
            df_tareo_for_merge["FECHA"] = pd.NaT

    df_postgres_lookup = df_postgres.copy()
    df_postgres_lookup["fecha"] = pd.to_datetime(df_postgres_lookup["fecha"], errors="coerce").dt.date
    df_postgres_lookup["area"] = df_postgres_lookup["area"].astype(str).str.strip().str.upper()

    df_tareo_for_merge["AREA2_tmp_UP"] = df_tareo_for_merge["AREA2_tmp"].astype(str).str.strip().str.upper()
    df_tareo_for_merge["FECHA"] = pd.to_datetime(df_tareo_for_merge["FECHA"], errors="coerce").dt.date

    # Intentar merge con la columna normalizada
    left_on_cols = ["FECHA", "AREA2_tmp_UP"] if "AREA2_tmp_UP" in df_tareo_for_merge.columns else ["FECHA", "AREA2_tmp"]

    df_merged = pd.merge(
        df_tareo_for_merge,
        df_postgres_lookup,
        left_on=left_on_cols,
        right_on=["fecha", "area"],
        how="left",
        suffixes=("_tareo", "_pg")
    )

    # ---------------- Aplicar la lógica de descomposición y distribución de horas ----------------
    registros_finales = []

    # Iteramos cada fila del merge y aplicamos reglas de negocio
    for _, row in df_merged.iterrows():
        he_d = row.get("HE_D", 0) or 0
        h_noche = row.get("H_NOCTURNAS", 0) or 0
        try:
            packing = float(row.get("packing", 0) or 0)
        except:
            packing = 0.0
        try:
            maquila = float(row.get("SERVICIO MAQUILA", 0) or 0)
        except:
            maquila = 0.0

        area_val = str(row.get("AREA", "")).strip()
        ceco_val = str(row.get("CECO", "Sin CECO")).strip()

        # CASOS según la lógica original
        if area_val in ["OBRAS EN CURSO", "GESTION DEL TALENTO HUMANO", "SSOMA"]:
            registros_finales.append({
                **row,
                "CECO_FINAL": ceco_val,
                "Horas_Dia": round(float(he_d), 2),
                "Horas_Noche": round(float(h_noche), 2)
            })
        elif area_val in ["PRODUCCION", "ALMACEN DE PISO PRODUCCION"]:
            registros_finales.append({
                **row,
                "CECO_FINAL": "PROCESO_PACK",
                "Horas_Dia": round(float(he_d * packing), 2),
                "Horas_Noche": round(float(h_noche * packing), 2)
            })
            registros_finales.append({
                **row,
                "CECO_FINAL": "SERV_MAQUILA",
                "Horas_Dia": round(float(he_d * maquila), 2),
                "Horas_Noche": round(float(h_noche * maquila), 2)
            })
        elif ceco_val == "RECEP_PACK":
            registros_finales.append({
                **row,
                "CECO_FINAL": "RECEP_PACK",
                "Horas_Dia": round(float(he_d * packing), 2),
                "Horas_Noche": round(float(h_noche * packing), 2)
            })
            registros_finales.append({
                **row,
                "CECO_FINAL": "SERV_MAQUILA",
                "Horas_Dia": round(float(he_d * maquila), 2),
                "Horas_Noche": round(float(h_noche * maquila), 2)
            })
        else:
            registros_finales.append({
                **row,
                "CECO_FINAL": ceco_val,
                "Horas_Dia": round(float(he_d * packing), 2),
                "Horas_Noche": round(float(h_noche * packing), 2)
            })
            registros_finales.append({
                **row,
                "CECO_FINAL": "SERV_MAQUILA",
                "Horas_Dia": round(float(he_d * maquila), 2),
                "Horas_Noche": round(float(h_noche * maquila), 2)
            })

    df_final = pd.DataFrame(registros_finales)

    # Asegurar Horas_Dia/Noche
    if not df_final.empty:
        df_final["Horas_Dia"] = df_final.get("Horas_Dia", 0).fillna(0).astype(float)
        df_final["Horas_Noche"] = df_final.get("Horas_Noche", 0).fillna(0).astype(float)
    else:
        df_final["Horas_Dia"] = pd.Series(dtype=float)
        df_final["Horas_Noche"] = pd.Series(dtype=float)

    # ---------------- Join con hoja DNI para traer FECHA_INGRESO y APELLIDOS ----------------
    df_final["N° DNI"] = df_final.get("N° DNI", df_final.get("N°DNI", "")).astype(str).str.strip()
    df_dni["DNI"] = df_dni["DNI"].astype(str).str.strip() if "DNI" in df_dni.columns else df_dni["DNI"]

    if not df_final.empty and not df_dni.empty:
        df_final = pd.merge(
            df_final,
            df_dni[["DNI", "FECHA_INGRESO", "APELLIDOS"]].drop_duplicates(subset=["DNI"]),
            left_on="N° DNI",
            right_on="DNI",
            how="left",
            suffixes=("", "_dni")
        )
    else:
        # Asegurar columnas si el merge no se hizo
        if "FECHA_INGRESO" not in df_final.columns:
            df_final["FECHA_INGRESO"] = pd.NaT
        if "APELLIDOS" not in df_final.columns:
            df_final["APELLIDOS"] = ""

    # ---------------- Join con hoja LABORES (por CODIGO) ----------------
    if "CODIGO" in df_final.columns:
        df_final["CODIGO"] = df_final["CODIGO"].astype(str).str.strip()
    else:
        if "COD" in df_final.columns:
            df_final["CODIGO"] = df_final["COD"].astype(str).str.strip()
        else:
            df_final["CODIGO"] = ""

    df_labores["CODIGO"] = df_labores.get("CODIGO", pd.Series(dtype=str)).astype(str).str.strip()

    if not df_labores.empty and not df_final.empty:
        df_final = pd.merge(
            df_final,
            df_labores[["CODIGO", "Labor", "ID_ACTIVIDAD", "COD_LABOR"]].drop_duplicates(subset=["CODIGO"]),
            left_on="CODIGO",
            right_on="CODIGO",
            how="left",
            suffixes=("", "_lab")
        )
    else:
        if "Labor" not in df_final.columns:
            df_final["Labor"] = ""
        if "ID_ACTIVIDAD" not in df_final.columns:
            df_final["ID_ACTIVIDAD"] = ""
        if "COD_LABOR" not in df_final.columns:
            df_final["COD_LABOR"] = ""

    # Asegurarnos de que ID_ACTIVIDAD y COD_LABOR sean texto y sin decimales
    if "ID_ACTIVIDAD" in df_final.columns:
        df_final["ID_ACTIVIDAD"] = df_final["ID_ACTIVIDAD"].apply(lambda x:"0"+ str(x).split(".")[0] if pd.notna(x) else "")
    else:
        df_final["ID_ACTIVIDAD"] = ""
    if "COD_LABOR" in df_final.columns:
        df_final["COD_LABOR"] = df_final["COD_LABOR"].apply(lambda x: safe_str(x))
    else:
        df_final["COD_LABOR"] = ""

    # Normalizar FECHA_INGRESO -> "F. INGRESO"
    if "FECHA_INGRESO" in df_final.columns:
        df_final["F. INGRESO"] = df_final["FECHA_INGRESO"]
    elif "F. INGRESO" in df_final.columns:
        df_final["F. INGRESO"] = pd.to_datetime(df_final["F. INGRESO"], errors="coerce").dt.date
    else:
        df_final["F. INGRESO"] = pd.NaT

    # APELLIDOS Y NOMBRES
    if "APELLIDOS" in df_final.columns and df_final["APELLIDOS"].notna().any():
        df_final["APELLIDOS Y NOMBRES"] = df_final["APELLIDOS"]
    else:
        if "APELLIDOS Y NOMBRES" in df_final.columns:
            df_final["APELLIDOS Y NOMBRES"] = df_final["APELLIDOS Y NOMBRES"]
        else:
            df_final["APELLIDOS Y NOMBRES"] = ""

    # ID-ACT y C_LAB (asegurando texto)

    df_final["ID-ACT"] = df_final.get("ID_ACTIVIDAD", "").apply(lambda x: str(x).split(".")[0] if pd.notna(x) else "")
    df_final["C_LAB"] = df_final.get("COD_LABOR", "").apply(lambda x: str(x).split(".")[0] if pd.notna(x) else "")



    # ---------------- Asegurar que exista DESCRIPCION DE LABOR para mostrarse en los cuadros ----------------
    # Llenamos "DESCRIPCION DE LABOR" desde la columna Labor si existe
    df_final["DESCRIPCION DE LABOR"] = df_final.get("Labor", df_final.get("DESCRIPCION DE LABOR", ""))

    # ---------------- Generar TXT DÍA / TXT NOCHE ----------------
    df_final["FECHA"] = pd.to_datetime(df_final.get("FECHA"), errors="coerce").dt.date

    def build_txt_row(row, turno="DIA"):
        fecha = row.get("FECHA")
        if pd.isna(fecha):
            year = ""
            month = ""
            day = ""
        else:
            year = fecha.year
            month = "{:02d}".format(fecha.month)
            day = "{:02d}".format(fecha.day)
        codigo_turno = "01" if turno == "DIA" else "03"
        dni = safe_str(row.get("N° DNI"))
        # usar ID-ACT ya normalizado como texto
        id_act = safe_str(row.get("ID-ACT"))
        c_lab = safe_str(row.get("C_LAB"))
        ceco = safe_str(row.get("CECO_FINAL"))
        horas = row.get("Horas_Dia", 0) if turno == "DIA" else row.get("Horas_Noche", 0)
        minutos = int(round(float(horas or 0) * 60))
        txt = f"0002|{year}{month}{day}|000004|{codigo_turno}|{dni}|{id_act}|{c_lab}|{ceco}|{minutos}|"
        return txt

    # Antes de aplicar build_txt_row, garantizar que ID-ACT y C_LAB estén como texto sin .0
    df_final["ID-ACT"] = df_final["ID-ACT"].apply(lambda x: safe_str(x))
    df_final["C_LAB"] = df_final["C_LAB"].apply(lambda x: safe_str(x))

    df_final["TXT DÍA"] = df_final.apply(lambda r: build_txt_row(r, "DIA"), axis=1)
    df_final["TXT NOCHE"] = df_final.apply(lambda r: build_txt_row(r, "NOCHE"), axis=1)

    # ---------------- Transformación a formato largo (Horas por TURNO) ----------------
    # Guardamos un identificador original para poder mapear filtros al resultado final
    df_final = df_final.reset_index(drop=True)
    df_final["_orig_idx"] = df_final.index

    # Asegurar que la columna DESCRIPCION DE LABOR existe antes del melt
    if "DESCRIPCION DE LABOR" not in df_final.columns:
        df_final["DESCRIPCION DE LABOR"] = ""

    df_long = pd.melt(
        df_final,
        id_vars=[c for c in df_final.columns if c not in ["Horas_Dia", "Horas_Noche"]],
        value_vars=["Horas_Dia", "Horas_Noche"],
        var_name="TURNO_FINAL",
        value_name="Horas"
    )

    df_long["TURNO_FINAL"] = df_long["TURNO_FINAL"].replace({
        "Horas_Dia": "DIA",
        "Horas_Noche": "NOCHE"
    })

    # ---------------- FILTROS (barra lateral) ----------------
    st.sidebar.header("🔎 Filtros")

    df_filtered = df_long.copy()

    # Variables para guardar los filtros que aplicaremos también al resultado final
    applied_filters = {}

    # Area (Excel)
    if "AREA" in df_filtered.columns:
        area_excel_filter = st.sidebar.multiselect("Área", sorted(df_filtered["AREA"].dropna().unique()))
        applied_filters['AREA'] = area_excel_filter
        if area_excel_filter:
            df_filtered = df_filtered[df_filtered["AREA"].isin(area_excel_filter)]

    # Grupo
    if "GRUPO" in df_filtered.columns:
        grupo_filter = st.sidebar.multiselect("Grupo", sorted(df_filtered["GRUPO"].dropna().unique()))
        applied_filters['GRUPO'] = grupo_filter
        if grupo_filter:
            df_filtered = df_filtered[df_filtered["GRUPO"].isin(grupo_filter)]

    # Fecha filter (rango / single)
    try:
        fecha_min = df_filtered["FECHA"].min()
        fecha_max = df_filtered["FECHA"].max()
    except:
        fecha_min = None
        fecha_max = None

    fecha_filter = st.sidebar.date_input("Fecha", [])
    applied_filters['FECHA'] = fecha_filter
    if fecha_filter:
        if isinstance(fecha_filter, (list, tuple)):
            df_filtered = df_filtered[df_filtered["FECHA"].isin(fecha_filter)]
        else:
            df_filtered = df_filtered[df_filtered["FECHA"] == fecha_filter
                                       ]

    # Nombre filter
    if "APELLIDOS Y NOMBRES" in df_filtered.columns:
        nombre_filter = st.sidebar.multiselect("Nombres", sorted(df_filtered["APELLIDOS Y NOMBRES"].dropna().unique()))
        applied_filters['APELLIDOS Y NOMBRES'] = nombre_filter
        if nombre_filter:
            df_filtered = df_filtered[df_filtered["APELLIDOS Y NOMBRES"].isin(nombre_filter)]

    # Validación filter (solo si existe campo)
    if "Validación" in df_filtered.columns:
        val_filter = st.sidebar.multiselect("Validación", sorted(df_filtered["Validación"].dropna().unique()))
        applied_filters['Validación'] = val_filter
        if val_filter:
            df_filtered = df_filtered[df_filtered["Validación"].isin(val_filter)]

    # ---------------- Limpiar columnas que no queremos mostrar ----------------
    columnas_excluir = [
        "TURNO", "EMPRESA", "Área correspondiente",
        "AREA2", "AREA2_tmp", "C DIA MAQUILA", "C DIA PACKING", "NOCHE MAQUILA", "NOCHE PAC",
        "Columna1", "SERVICIO DE MAQUILA GTH", "PACKING GTH", "2024", "fecha", "area",
        "packing", "SERVICIO MAQUILA", "HE_D", "H_NOCTURNAS", "Total de horas",
        "BONO FRIO", "BONO RESPONSABILIDAD", "BONO MOVILIDAD", "CECO"
    ]
    df_filtered = df_filtered.drop(columns=[c for c in columnas_excluir if c in df_filtered.columns], errors="ignore")

    # ---------------- Reordenar columnas para mostrar primer cuadro ----------------
    if "TURNO_FINAL" in df_filtered.columns:
        cols = ["TURNO_FINAL"] + [c for c in df_filtered.columns if c != "TURNO_FINAL"]
    else:
        cols = list(df_filtered.columns)
    df_filtered = df_filtered[cols]

    # Reinsertar CECO_FINAL y Horas justo después de APELLIDOS Y NOMBRES si existen
    if "APELLIDOS Y NOMBRES" in df_filtered.columns and {"CECO_FINAL", "Horas"}.issubset(df_filtered.columns):
        cols = list(df_filtered.columns)
        if "CECO_FINAL" in cols:
            cols.remove("CECO_FINAL")
        if "Horas" in cols:
            cols.remove("Horas")
        if "APELLIDOS Y NOMBRES" in cols:
            idx = cols.index("APELLIDOS Y NOMBRES") + 1
            cols = cols[:idx] + ["CECO_FINAL", "Horas"] + cols[idx:]
            df_filtered = df_filtered[cols]

    # ---------------- Primer cuadro: resultados distribuidos (long) ----------------
    st.subheader("📋 Resumen - Turno en filas")
    st.dataframe(df_filtered, use_container_width=True, hide_index=True)

    # ---------------- Segundo cuadro: resumen sin TURNO_FINAL (pivot) - Horas_Dia/Horas_Noche ----------------
    df_third = None
    if {"FECHA", "APELLIDOS Y NOMBRES", "Horas", "TURNO_FINAL", "CECO_FINAL"}.issubset(df_filtered.columns):
        pivot_index = [c for c in df_filtered.columns if c not in ["Horas", "TURNO_FINAL"]]
        try:
            df_third = df_filtered.pivot_table(
                index=pivot_index,
                columns="TURNO_FINAL",
                values="Horas",
                aggfunc="sum",
                fill_value=0
            ).reset_index()

            # Renombrar columnas DÍA/NOCHE
            if "DIA" in df_third.columns:
                df_third = df_third.rename(columns={"DIA": "Horas_Dia"})
            if "NOCHE" in df_third.columns:
                df_third = df_third.rename(columns={"NOCHE": "Horas_Noche"})

            # Reubicar Horas_Dia y Horas_Noche justo después de CECO_FINAL
            if "CECO_FINAL" in df_third.columns:
                cols = list(df_third.columns)
                for c in ["Horas_Dia", "Horas_Noche"]:
                    if c in cols:
                        cols.remove(c)
                idx = cols.index("CECO_FINAL") + 1
                cols = cols[:idx] + ["Horas_Dia", "Horas_Noche"] + cols[idx:]
                df_third = df_third[cols]

            st.subheader("📊 Resumen - Turno en columnas")
            st.dataframe(df_third, use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(f"No fue posible pivotear el dataframe: {e}")
            df_third = None


    # ---------------- SINCRONIZAR FILTROS CON 'RESULTADO FINAL' ----------------
    if "_orig_idx" in df_filtered.columns:
        orig_idx_set = df_filtered["_orig_idx"].unique().tolist()
    else:
        orig_idx_set = []

    # ---------------- Tercer cuadro - Construir resultado final con el orden de columnas solicitado ----------------
    out = df_final.copy()

    # Aplicar filtro de _orig_idx al resultado final para sincronizar
    if orig_idx_set:
        out = out[out["_orig_idx"].isin(orig_idx_set)].copy()

    # Normalizar nombres de columnas solicitadas. Asegurar existencia:
    out["AREA"] = out.get("AREA", "")
    out["GRUPO"] = out.get("GRUPO", "")
    if "COD" not in out.columns:
        out["COD"] = out.get("COD", "")
    if "SEM" not in out.columns:
        out["SEM"] = out.get("SEM", "")
    out["FECHA"] = pd.to_datetime(out.get("FECHA"), errors="coerce").dt.date
    out["CODIGO"] = out.get("CODIGO", "").astype(str).str.strip()
    out["DESCRIPCION DE LABOR"] = out.get("DESCRIPCION DE LABOR", out.get("Labor", ""))
    out["CECO_FINAL"] = out.get("CECO_FINAL", "")
    out["F. INGRESO"] = out.get("F. INGRESO", pd.NaT)
    out["N° DNI"] = out.get("N° DNI", "").astype(str).str.strip()
    out["APELLIDOS Y NOMBRES"] = out.get("APELLIDOS Y NOMBRES", "")
    out["Horas_Dia"] = out.get("Horas_Dia", 0).astype(float) if "Horas_Dia" in out.columns else 0.0
    out["Horas_Noche"] = out.get("Horas_Noche", 0).astype(float) if "Horas_Noche" in out.columns else 0.0
    out["ID-ACT"] = out.get("ID-ACT", "").astype(str).str.strip()
    out["ID-ACT-FINAL"] = out.get("ID-ACT", "").astype(str).str.strip()
    out["C_LAB"] = out.get("C_LAB", "").astype(str).str.strip()
    out["TXT DÍA"] = out.get("TXT DÍA", "")
    out["TXT NOCHE"] = out.get("TXT NOCHE", "")

    final_columns_order = [
        "AREA", "GRUPO", "COD", "SEM", "FECHA", "CODIGO", "DESCRIPCION DE LABOR",
        "CECO_FINAL", "F. INGRESO", "N° DNI", "APELLIDOS Y NOMBRES",
        "Horas_Dia", "Horas_Noche", "ID-ACT-FINAL", "C_LAB", "TXT DÍA", "TXT NOCHE"
    ]

    for c in final_columns_order:
        if c not in out.columns:
            out[c] = ""

    df_result_final = out[final_columns_order].copy()

    # asegurar decimales en Horas
    df_result_final["Horas_Dia"] = df_result_final["Horas_Dia"].fillna(0).astype(float).round(2)
    df_result_final["Horas_Noche"] = df_result_final["Horas_Noche"].fillna(0).astype(float).round(2)

    # ---------------- Mostrar el Resultado Final (sincronizado con filtros) ----------------
    st.subheader("✅ Resumen final (según correo)")
    st.dataframe(df_result_final, use_container_width=True, hide_index=True)

    # ---------------- Cuarto cuadro: Validación por FECHA, AREA, APELLIDOS Y NOMBRES ----------------
    df_summary_tot = None
    if {"FECHA", "AREA", "APELLIDOS Y NOMBRES", "Horas", "TURNO_FINAL"}.issubset(df_filtered.columns):
        # Usar pivot para evitar problemas con closures dentro de agg
        df_pivot = df_filtered.pivot_table(
            index=["FECHA", "AREA", "APELLIDOS Y NOMBRES"],
            columns="TURNO_FINAL",
            values="Horas",
            aggfunc="sum",
            fill_value=0
        ).reset_index()

        df_pivot.columns.name = None
        df_pivot = df_pivot.rename(columns={"DIA": "Horas_Dia", "NOCHE": "Horas_Noche"})
        if "Horas_Dia" not in df_pivot.columns:
            df_pivot["Horas_Dia"] = 0
        if "Horas_Noche" not in df_pivot.columns:
            df_pivot["Horas_Noche"] = 0

        df_pivot["Horas"] = (df_pivot["Horas_Dia"] + df_pivot["Horas_Noche"]).round(1)
        df_pivot["Horas_Dia"] = df_pivot["Horas_Dia"].round(1)
        df_pivot["Horas_Noche"] = df_pivot["Horas_Noche"].round(1)
        df_pivot["HorasValidacion"] = (df_pivot["Horas_Dia"] + df_pivot["Horas_Noche"]).round(1)
        df_pivot["Validación"] = df_pivot.apply(
            lambda r: "CORRECTO" if r["Horas"] == r["HorasValidacion"] else "INCORRECTO",
            axis=1
        )

        # filtro adicional por Validación
        validacion_filter = st.sidebar.multiselect("Validación", sorted(df_pivot["Validación"].unique()))
        if validacion_filter:
            df_pivot = df_pivot[df_pivot["Validación"].isin(validacion_filter)]

        # fila total
        total_row = pd.DataFrame({
            "FECHA": ["TOTAL"],
            "AREA": [""],
            "APELLIDOS Y NOMBRES": [""],
            "Horas_Dia": [df_pivot["Horas_Dia"].sum().round(1)],
            "Horas_Noche": [df_pivot["Horas_Noche"].sum().round(1)],
            "Horas": [df_pivot["Horas"].sum().round(1)],
            "HorasValidacion": [df_pivot["HorasValidacion"].sum().round(1)],
            "Validación": [""]
        })
        df_summary_tot = pd.concat([df_pivot, total_row], ignore_index=True)


    st.subheader("📊 Validación por fecha, área y apellidos")
    st.dataframe(df_summary_tot, use_container_width=True, hide_index=True)

    # ---------------- Descargar resultados ----------------
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        try:
            df_tareo.to_excel(writer, index=False, sheet_name="Datos de Usuario GTH")
        except Exception:
            pass
        try:
            df_result_final.to_excel(writer, index=False, sheet_name="Resumen final (según correo)")
        except Exception:
            pass
        if df_summary_tot is not None:
            try:
                df_summary_tot.to_excel(writer, index=False, sheet_name="Validacion")
            except Exception:
                pass
        if df_third is not None:
            try:
                df_third.to_excel(writer, index=False, sheet_name="Resumen - Turno en columnas")
            except Exception:
                pass
        try:
            df_merged.to_excel(writer, index=False, sheet_name="%Kilos de Zupra")
        except Exception:
            pass

    st.download_button(
        label="📥 Exportar la distribución",
        data=output.getvalue(),
        file_name="Sistemas de distribución de horas.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Sube la estructura correcta en excel.")


    #PARA OCULTAR HECHO POR STREAMLIT Y MENU DEPLOY
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)


