# -*- coding: utf-8 -*-
"""
Maestro de funcionarios de Prize: quién es cada DNI dentro de la empresa.

El export de qbiz trae una fila por contrato y TODOS los datos metidos en una
columna `payload` en JSON. Aquí se abre ese JSON, se deja una sola fila por
DNI (la vigente, y entre varias la más reciente) y se cruza con el padrón del
filtro de antecedentes.

Un DNI del filtro que no aparece en el maestro se marca como
«NO PERTENECE A PRIZE»: es el dato que pide el comité para saber si la persona
está realmente en planilla o llegó por una contrata.
"""
from __future__ import annotations

import glob
import json
import os
import re

import pandas as pd

VERSION = "3.2"   # debe coincidir con filtro/config.py

SIN_MATCH = "NO PERTENECE A PRIZE"

# clave del JSON -> columna del tablero
CAMPOS = {
    "COD FUNCIONARIO":  "COD_FUNCIONARIO",
    "EMPRESA":          "EMPRESA",
    "COLABORADOR":      "COLABORADOR",
    "APELLIDO PATERNO": "APELLIDO_PATERNO",
    "APELLIDO MATERNO": "APELLIDO_MATERNO",
    "NOMBRES":          "NOMBRES",
    "CARGO":            "CARGO",
    "ÁREA":             "AREA",
    "AREA":             "AREA",
    "CENTROCOSTO":      "CENTRO_COSTO",
    "RÉGIMEN":          "REGIMEN",
    "REGIMEN":          "REGIMEN",
    "TIPO TRABAJADOR":  "TIPO_TRABAJADOR",
    "PLANILLA NISIRA":  "PLANILLA",
    "VIGENCIA":         "VIGENCIA_PLANILLA",
    "FECHA DE INGRESO": "FECHA_INGRESO",
    "FECHA DE CESE":    "FECHA_CESE",
    "FECHAMODIF":       "FECHA_MODIF",
}

# columnas que se añaden al padrón, en el orden en que se muestran
COLUMNAS = ["EN_PRIZE", "ESTADO_PRIZE", "EMPRESA", "COD_FUNCIONARIO", "COLABORADOR",
            "CARGO", "AREA", "CENTRO_COSTO", "REGIMEN", "TIPO_TRABAJADOR", "PLANILLA",
            "VIGENCIA_PLANILLA", "FECHA_INGRESO", "FECHA_CESE", "ANTIGUEDAD_ANIOS",
            "N_CONTRATOS", "FECHA_MODIF"]

# columnas de texto que, sin match, llevan el aviso completo
CON_AVISO = ("ESTADO_PRIZE", "EMPRESA", "CARGO", "AREA")
# columnas numéricas: sin match quedan vacías, no con el aviso
NUMERICAS = ("N_CONTRATOS", "ANTIGUEDAD_ANIOS")

PATRONES_ARCHIVO = ("*funcionario*.csv", "*funcionario*.xlsx", "*maestro*.csv",
                    "*maestro*.xlsx", "*planilla*.csv", "*planilla*.xlsx",
                    "*qbiz*.csv", "*qbiz*.xlsx")
NOMBRE_GUARDADO = "maestro_prize.csv"     # copia que deja el cargador del tablero
GUARDADOS = ("maestro_prize.csv", "maestro_prize.xlsx", "maestro_prize.xlsm")


# ------------------------------------------------------------------ utilidades
def clave_dni(v) -> str:
    """
    Normaliza un DNI para comparar: solo dígitos y sin ceros a la izquierda.
    Así «07654321», «7654321» y «7654321.0» son la misma persona.
    """
    s = str(v if v is not None else "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    s = re.sub(r'\D', '', s)
    return s.lstrip("0")


def _fecha(v):
    try:
        return pd.to_datetime(v, errors='coerce')
    except Exception:
        return pd.NaT


def parece_maestro(nombre: str) -> bool:
    """
    ¿Este archivo suelto es el export de funcionarios y no un Resumen de cuadrilla?
    Sirve para clasificar lo que se sube desde el navegador.
    """
    base = os.path.basename(str(nombre or ""))
    raiz_, ext = os.path.splitext(base)
    if ext.lower() not in (".csv", ".xlsx", ".xlsm"):
        return False
    return not raiz_.upper().startswith("RESUMEN_NEW_VIP")


def localizar(raiz: str) -> str | None:
    """Busca el maestro dentro de la carpeta de trabajo. Devuelve la ruta o None."""
    if not raiz or not os.path.isdir(raiz):
        return None
    for nombre in GUARDADOS:
        guardado = os.path.join(raiz, ".cache_filtro", nombre)
        if os.path.isfile(guardado):
            return guardado
    encontrados = []
    for patron in PATRONES_ARCHIVO:
        encontrados += [p for p in glob.glob(os.path.join(raiz, patron))
                        if not os.path.basename(p).startswith("~$")]
    if not encontrados:
        return None
    return max(encontrados, key=lambda p: os.path.getmtime(p))


# ------------------------------------------------------------------ lectura
def _abrir(origen) -> pd.DataFrame:
    """Lee el export tal cual, sea .csv o .xlsx, ruta o archivo subido."""
    nombre = origen if isinstance(origen, str) else getattr(origen, "name", "")
    if str(nombre).lower().endswith((".xlsx", ".xlsm")):
        return pd.read_excel(origen, dtype=str)
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            if hasattr(origen, "seek"):
                origen.seek(0)
            return pd.read_csv(origen, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise ValueError("No se pudo leer el archivo: codificación desconocida.")


def _expandir_payload(df: pd.DataFrame) -> pd.DataFrame:
    """
    Si hay una columna con JSON (payload), la abre en columnas.
    Si el archivo ya viene plano, lo deja como está.
    """
    col_json = None
    for c in df.columns:
        if str(c).strip().lower() in ("payload", "json", "data", "datos"):
            col_json = c
            break
    if col_json is None:                      # ¿alguna columna parece JSON?
        for c in df.columns:
            m = df[c].dropna().astype(str).head(20)
            if len(m) and (m.str.strip().str.startswith("{")).mean() > 0.8:
                col_json = c
                break
    if col_json is None:
        return df

    filas = []
    for i, txt in enumerate(df[col_json]):
        try:
            d = json.loads(txt) if isinstance(txt, str) else {}
        except (ValueError, TypeError):
            d = {}
        if not isinstance(d, dict):
            d = {}
        base = {k: v for k, v in df.iloc[i].items() if k != col_json}
        base.update(d)
        filas.append(base)
    return pd.DataFrame(filas)


def cargar(origen) -> pd.DataFrame:
    """
    Devuelve un DataFrame con UNA fila por DNI y las columnas de `CAMPOS`.

    Cuando un DNI trae varios contratos se conserva el más relevante:
    primero el vigente, y entre ellos el de ingreso / modificación más reciente.
    """
    crudo = _expandir_payload(_abrir(origen))
    if not len(crudo):
        return pd.DataFrame(columns=["_K"] + COLUMNAS)

    # columnas del JSON -> nombres del tablero (sin distinguir mayúsculas ni tildes)
    ren = {}
    for c in crudo.columns:
        k = str(c).strip().upper()
        if k in CAMPOS:
            ren[c] = CAMPOS[k]
        elif k == "DNI":
            ren[c] = "DNI"
    d = crudo.rename(columns=ren)
    # El export trae la empresa (y el código) dos veces: como columna suelta del
    # CSV y dentro del JSON. Al renombrar quedan dos columnas con el mismo
    # nombre; nos quedamos con la del JSON, que es la que viene completa.
    d = d.loc[:, ~d.columns.duplicated(keep="last")]
    if "DNI" not in d.columns:
        raise ValueError("El archivo no tiene una columna DNI ni dentro del JSON.")

    for col in set(CAMPOS.values()) | {"DNI"}:
        if col not in d.columns:
            d[col] = ""
    d = d[["DNI"] + sorted(set(CAMPOS.values()))].copy()
    for c in d.columns:
        d[c] = d[c].astype(str).replace({"nan": "", "None": "", "NaT": ""}).str.strip()

    d["_K"] = d["DNI"].map(clave_dni)
    d = d[d["_K"] != ""].copy()

    # ---- cuántos contratos tiene cada DNI, y cuál se queda ----
    d["N_CONTRATOS"] = d.groupby("_K")["_K"].transform("size")
    d["_VIG"] = (d["VIGENCIA_PLANILLA"].str.upper().str[:2] == "SI").astype(int)
    d["_ING"] = _fecha(d["FECHA_INGRESO"])
    d["_MOD"] = _fecha(d["FECHA_MODIF"])
    d = (d.sort_values(["_K", "_VIG", "_MOD", "_ING"],
                       ascending=[True, False, False, False], na_position="last")
          .drop_duplicates("_K", keep="first"))

    # ---- campos derivados ----
    hoy = pd.Timestamp.today().normalize()
    fin = _fecha(d["FECHA_CESE"]).fillna(hoy)
    d["ANTIGUEDAD_ANIOS"] = ((fin - d["_ING"]).dt.days / 365.25).round(1)
    d["EN_PRIZE"] = "SI"
    d["ESTADO_PRIZE"] = d["_VIG"].map({1: "ACTIVO EN PLANILLA", 0: "CESADO"})
    d.loc[d["VIGENCIA_PLANILLA"] == "", "ESTADO_PRIZE"] = "SIN DATO DE VIGENCIA"
    return d[["_K"] + COLUMNAS].reset_index(drop=True)


# ------------------------------------------------------------------ cruce
def aplicar(df_p: pd.DataFrame, maestro: pd.DataFrame | None) -> tuple[pd.DataFrame, dict]:
    """
    Añade las columnas del maestro al padrón cruzando por DNI.

    Devuelve (df_p enriquecido, resumen del cruce). Si no hay maestro, todas
    las personas quedan marcadas como «sin dato» y el tablero lo dice.
    """
    df = df_p.copy()
    # Si el Excel de la cuadrilla ya trae una columna con el mismo nombre
    # (CARGO, ÁREA, EMPRESA…), se conserva renombrada: el dato de origen no
    # se pisa nunca sin dejar rastro.
    choques = {c: f"{c}_ORIGEN" for c in COLUMNAS if c in df.columns}
    if choques:
        df = df.rename(columns=choques)
    info = dict(hay_maestro=maestro is not None and len(maestro) > 0,
                filas_maestro=0 if maestro is None else len(maestro),
                con_match=0, sin_match=len(df), activos=0, cesados=0,
                renombradas=sorted(choques.values()))

    if not info["hay_maestro"]:
        for c in COLUMNAS:
            df[c] = pd.NA if c in NUMERICAS else ""
        df["EN_PRIZE"] = "SIN DATO"
        df["ESTADO_PRIZE"] = "SIN MAESTRO CARGADO"
        return df, info

    m = maestro.drop_duplicates("_K").set_index("_K")
    claves = df["DNI"].map(clave_dni)
    for c in COLUMNAS:
        df[c] = claves.map(m[c]) if c in m.columns else ""

    falta = ~claves.isin(m.index)
    for c in COLUMNAS:
        if c in NUMERICAS:                    # sin match ya quedan en NaN
            df[c] = pd.to_numeric(df[c], errors="coerce")
            continue
        df[c] = df[c].astype(object).where(~falta,
                                           SIN_MATCH if c in CON_AVISO else "")
        df[c] = df[c].fillna("")
    df.loc[falta, "EN_PRIZE"] = "NO"

    info.update(con_match=int((~falta).sum()), sin_match=int(falta.sum()),
                activos=int((df["ESTADO_PRIZE"] == "ACTIVO EN PLANILLA").sum()),
                cesados=int((df["ESTADO_PRIZE"] == "CESADO").sum()))
    return df, info
