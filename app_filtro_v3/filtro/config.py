# -*- coding: utf-8 -*-
"""
Configuración única del filtro de antecedentes Prize / Aquanqa.

Todo lo que define QUÉ se lee y CÓMO se clasifica vive aquí.
Si el comité cambia un criterio, se cambia en este archivo y nada más.
"""
from __future__ import annotations

# Versión del paquete. app.py comprueba que TODOS los módulos coincidan:
# un despliegue a medias (unos archivos nuevos y otros viejos) es la causa
# más común de errores raros, y así se detecta con un mensaje claro.
VERSION = "3.4"

# ---------------------------------------------------------------- rutas
import os as _os

# Carpeta que se abre por defecto. Se puede sobrescribir sin tocar el código
# definiendo la variable de entorno FILTRO_RAIZ.
# Fuera de Windows, C:\FILTER no significa nada (y al unirlo a otra ruta produce
# rarezas como «/mount/src/repo/C:\FILTER»), así que se usa una ruta del sistema.
def _carpeta_inicial() -> str:
    env = _os.environ.get("FILTRO_RAIZ")
    if env:
        return env
    if _os.name == "nt":
        return r"C:\FILTER"
    return _os.path.join(_os.path.expanduser("~"), "FILTER")


CARPETA_POR_DEFECTO = _carpeta_inicial()
PATRON_EXCEL   = "Resumen_NEW_VIP_*.xlsx"   # dentro de cada carpeta de cuadrilla
SUBCARPETA_PDF = "Adjuntos"                 # dentro de cada carpeta de cuadrilla
PATRON_PDF     = "*.pdf"
NOMBRE_SALIDA  = "resultado_final.xlsx"

# Límite de subida de la versión web. Debe coincidir con
# .streamlit/config.toml -> [server] maxUploadSize.
MAX_SUBIDA_MB  = 300
HOJA_EXCEL     = "Hoja1"
COL_DNI        = "DNI"
COL_NOMBRE     = "APELLIDOS Y NOMBRES"

# ------------------------------------------------- matriz de criticidad
# nivel -> (concepto de referencia, acción, criticidad)
NIVELES = {
    1: ("EXTORSION", "RETIRO", "CRITICO"),
    2: ("ROBO, HURTO, HOMICIDIO, ORGANIZACION CRIMINAL, FRAUDE, BANDA", "RETIRO", "CRITICO"),
    3: ("TEMAS SEXUALES (PROXENETISMO, RUFIANISMO, VIOLACION SEXUAL, OTROS ACTOS)", "RETIRO", "CRITICO"),
    4: ("RESISTENCIA A LA AUTORIDAD, CONDUCCION EN EBRIEDAD", "SE ESTUDIA SALIDA", "ALTO"),
    5: ("FAMILIA (ALIMENTOS, VIOLENCIA DOMESTICA, PENSIONES)", "SE ESTUDIA SALIDA", "MEDIO"),
    6: ("LEVES (PELEAS, RIÑAS, DAÑOS MENORES, OTROS)", "SE ESTUDIA SALIDA", "BAJO"),
}

# ------------------------------------------------------- reglas N1..N6
# (nivel, categoría, [patrones regex])  -- SE EVALÚAN EN ESTE ORDEN.
# Gana el primer patrón que coincide: por eso "LESIONES LEVES (AGRESIONES EN
# CONTRA DE LAS MUJERES...)" cae en N5 familia y no en N6 lesiones.
RULES = [
    (1, "Extorsión", [r'\bEXTORSION', r'CHANTAJE', r'SECUESTRO EXTORSIVO']),
    (3, "Delitos sexuales", [r'VIOLACION SEXUAL', r'VIOLACION DE LA LIBERTAD SEXUAL', r'\bV\.L\.S\b',
                             r'PROXENETISMO', r'RUFIANISMO', r'ACTOS CONTRA EL PUDOR', r'TOCAMIENTOS',
                             r'TRATA DE PERSONAS', r'PORNOGRAFIA', r'SEDUCCION']),
    (5, "Violencia familiar y contra la mujer", [r'AGRESIONES EN CONTRA DE LAS MUJERES',
                             r'AGRESIONES EN CONTRA DE LA MUJER', r'VIOLENCIA FAMILIAR',
                             r'VIOLENCIA DOMESTICA', r'VIOLENCIA CONTRA LA MUJER', r'MALTRATO FISICO',
                             r'MALTRATO PSICOLOGICO', r'MALTRATO SICOLOGICO', r'INDUCCION A LA FUGA DE MENOR']),
    (5, "Omisión de alimentos", [r'ALIMENT', r'\bO\.A\.F', r'PENSION']),
    (2, "Homicidio", [r'HOM\.CAL', r'HOMICIDIO CALIFICADO', r'ASESINATO', r'SICARIATO',
                      r'FEMINICIDIO', r'PARRICIDIO']),
    (4, "Homicidio culposo / tránsito", [r'HOMICIDIO.*CULPOSO', r'CULPOSO.*HOMICIDIO', r'^CULPOSO$',
                                         r'\b111 HOMICIDIO CULPOSO']),
    (2, "Contra el patrimonio", [r'\bROBO', r'\bHURTO', r'RECEPTACION', r'USURPACION', r'ABIGEATO',
                                 r'CONTRA EL PATRIMONIO', r'\b189\b', r'MARCAJE', r'REGLAJE']),
    (2, "Fraude / falsificación", [r'ESTAFA', r'FRAUDE', r'FALSIFICACION', r'DEFRAUDACION',
                                   r'APROPIACION ILICITA']),
    (2, "Organización criminal", [r'ORGANIZACION CRIMINAL', r'\bBANDA\b', r'ASOCIACION ILICITA']),
    (2, "Secuestro / libertad personal", [r'SECUESTRO']),
    (2, "Drogas", [r'TRAFICO ILICITO DE DROGAS', r'MICROCOMERCIALIZACION', r'PROMOCION.*DROGA',
                   r'\bT\.I\.D\.\s*\(MICRO']),
    (2, "Armas y explosivos", [r'EXPLOSIVOS', r'TENENCIA ILEGAL DE ARMA', r'ARMA DE FUEGO']),
    (4, "Contra la autoridad", [r'RESISTENCIA', r'DESOB', r'\bV\.R\.A\b',
                                r'ATENTADO CONTRA LA AUTORIDAD', r'VIOLENCIA Y RESISTENCIA']),
    (4, "Conducción en ebriedad", [r'EBRIEDAD', r'DROGADICCION', r'IMPRUDENC\.CONDUCCION',
                                   r'D\.P\.C\.\s*\(CONDUCCION', r'CONDUCCION.*VEHICULO']),
    (6, "Lesiones", [r'LESIONES', r'LES\.GRAV']),
    (6, "Daños", [r'\bDAÑO', r'\bDANO']),
    (6, "Consumo de drogas", [r'CONSUMO PERSONAL']),
    (6, "Ambiental / forestal", [r'FORESTAL', r'TIERRAS AGRICOLAS', r'AMBIENT']),
    (6, "Salud pública / sanitaria", [r'SANITARIA', r'SALUD PUBLICA']),
    (6, "Transporte / servicios públicos", [r'PERTURB', r'TRANSP\.S\.P']),
    (6, "Coacción", [r'COACCION']),
]
CATEGORIA_SIN_MATCH = "Otros / genérico"
NIVEL_SIN_MATCH = 6

# -------------------------------------- gravedad jurídica por categoría
# (independiente del nivel de la matriz: alimenta el índice de riesgo)
GRAV_CAT = {
    "Extorsión": "GRAVE", "Delitos sexuales": "GRAVE", "Homicidio": "GRAVE",
    "Contra el patrimonio": "GRAVE", "Fraude / falsificación": "GRAVE",
    "Organización criminal": "GRAVE", "Secuestro / libertad personal": "GRAVE",
    "Drogas": "GRAVE", "Armas y explosivos": "GRAVE",
    "Violencia familiar y contra la mujer": "MEDIO", "Lesiones": "MEDIO",
    "Contra la autoridad": "MEDIO", "Conducción en ebriedad": "MEDIO",
    "Homicidio culposo / tránsito": "MEDIO",
}
GRAVEDAD_POR_DEFECTO = "LEVE"
PESO_GRAV = {"GRAVE": 4, "MEDIO": 2, "LEVE": 1}

# --------------------------------------------- índice de riesgo 0..100
IDX_GRAVEDAD     = {"GRAVE": 40, "MEDIO": 22, "LEVE": 10}
IDX_VIGENCIA     = [(2023, 20), (2018, 10)]   # año >= corte -> puntos; si no, resto
IDX_VIGENCIA_OLD = 2                          # hay año pero anterior a 2018
IDX_REINCIDENCIA = {1: 0, 2: 7, 3: 13}        # 4 o más -> 20
IDX_REINCIDENCIA_MAX = 20
IDX_CASO_ACTIVO  = 12
IDX_DISPERSION   = 8                          # 2 o más jurisdicciones
ANIO_VIGENCIA    = 2023

# estados que indican caso CERRADO (no cuentan como activo)
ACTIVO_NEG = ('ARCHIVO', 'SOBRESEI', 'ABSUELTO', 'RESERVA', 'EXTIN',
              'PRESCRI', 'NO HA LUGAR', 'INHIBI', 'DERIVA')

# ------------------------------------------ carpeta -> labor / personal
# El primer fragmento que aparezca en el nombre de la carpeta manda, y se
# evalúan EN ESTE ORDEN: por eso "BUS" va antes que "CHOFER" — una carpeta
# «CHOFERES DE BUSES» debe caer en CHOFER BUS y no en CHOFER KIA.
LABOR_MAP = [
    ("PACKING",     "PACKING",               "PROPIO"),
    ("ESTIBA",      "ESTIBADOR",             "PROPIO"),
    ("SEGURIDAD",   "SEGURIDAD PATRIMONIAL", "PROPIO"),
    ("PATRIMONIAL", "SEGURIDAD PATRIMONIAL", "PROPIO"),
    ("BUS",         "CHOFER BUS",            "TERCERO"),
    ("CHOFER",      "CHOFER KIA",            "TERCERO"),
    ("KIA",         "CHOFER KIA",            "TERCERO"),
    ("CAMPO",       "CAMPO",                 "PROPIO"),
]
LABOR_SIN_MATCH = ("NO DEFINIDO", "NO DEFINIDO")

# --------------------------------------------------------- veredictos
VEREDICTOS = ["NO APTO", "REVISION EN COMITE", "APTO CON OBSERVACION",
              "APTO", "PENDIENTE DE REPORTE"]

# ------------------------------------------------------------- colores
COLOR = {
    "NO APTO":              "#C0332E",
    "REVISION EN COMITE":   "#E8A317",
    "APTO CON OBSERVACION": "#C07C11",
    "APTO":                 "#1F7A3D",
    "PENDIENTE DE REPORTE": "#8A8F8B",
}
COLOR_NIVEL = {1: "#C0332E", 2: "#C0332E", 3: "#C0332E",
               4: "#E8A317", 5: "#7E8C33", 6: "#7E8C33"}
COLOR_GRAVEDAD = {"GRAVE": "#C0332E", "MEDIO": "#E8A317", "LEVE": "#7E8C33"}
COLOR_LABOR = {"ESTIBADOR": "#1F5C8B", "CHOFER KIA": "#A0522D",
               "PACKING": "#5B6E2F", "SEGURIDAD PATRIMONIAL": "#6B4E9E",
               "CHOFER BUS": "#2E7D7A", "CAMPO": "#8C7A1F",
               "NO DEFINIDO": "#8A8F8B"}
BRAND   = "#17564A"
CRIT    = "#C0332E"
WARN    = "#E8A317"
GOOD    = "#1F7A3D"
IDLE    = "#8A8F8B"

# ------------------------------- categorías fuera de la matriz N1-N6
# Las personas sin ningún delito en rojo no tienen nivel, pero deben aparecer
# junto a N1-N6 para que la suma cierre con el total del padrón.
#   clave -> (etiqueta corta, concepto, acción, color, veredicto que la origina)
SIN_OBSERVACIONES = "SIN OBSERVACIONES"
SIN_VERIFICAR     = "SIN VERIFICAR"

EXTRA_MATRIZ = {
    SIN_OBSERVACIONES: dict(
        etiqueta="Sin obs.",
        concepto="CON REPORTE ADJUNTO Y NINGÚN REGISTRO EN ROJO",
        accion="NINGUNA",
        color=GOOD,
        fg="#FFFFFF",
        veredicto="APTO"),
    SIN_VERIFICAR: dict(
        etiqueta="Sin verif.",
        concepto="SIN REPORTE ADJUNTO EN LA CARPETA ADJUNTOS (PUNTO CIEGO)",
        accion="SOLICITAR REPORTE",
        color=IDLE,
        fg="#FFFFFF",
        veredicto="PENDIENTE DE REPORTE"),
}

# Orden de criticidad: 1 = lo más crítico. Sirve para ordenar el padrón.
ORDEN_VEREDICTO = {v: i + 1 for i, v in enumerate(VEREDICTOS)}


# ============================================================ temas
# Los mismos significados con dos afinaciones: sobre papel claro los colores
# van saturados y oscuros; sobre fondo oscuro se suben en luminosidad para que
# mantengan contraste. Cambiar de tema reescribe estas variables del módulo,
# así que todo lo que lea C.CRIT (tablero, gráficos, tarjetas) se adapta solo.
TEMAS = {
    "claro": dict(
        BRAND="#17564A", CRIT="#C0332E", WARN="#E8A317", GOOD="#1F7A3D",
        IDLE="#8A8F8B", OLIVE="#7E8C33", AMBAR2="#C07C11"),
    "oscuro": dict(
        BRAND="#3FBBA0", CRIT="#F0736A", WARN="#F5B93E", GOOD="#5FC47E",
        IDLE="#9AA3AC", OLIVE="#B0C056", AMBAR2="#E0A73F"),
}
TEMA = "claro"


def aplicar_tema(nombre: str = "claro") -> None:
    """Reescribe la paleta del módulo según el tema elegido."""
    global TEMA, BRAND, CRIT, WARN, GOOD, IDLE, OLIVE, AMBAR2
    global COLOR, COLOR_NIVEL, COLOR_GRAVEDAD, COLOR_LABOR, EXTRA_MATRIZ
    t = TEMAS.get(nombre, TEMAS["claro"])
    TEMA = nombre if nombre in TEMAS else "claro"
    BRAND, CRIT, WARN = t["BRAND"], t["CRIT"], t["WARN"]
    GOOD, IDLE, OLIVE, AMBAR2 = t["GOOD"], t["IDLE"], t["OLIVE"], t["AMBAR2"]

    COLOR = {"NO APTO": CRIT, "REVISION EN COMITE": WARN,
             "APTO CON OBSERVACION": AMBAR2, "APTO": GOOD,
             "PENDIENTE DE REPORTE": IDLE}
    COLOR_NIVEL = {1: CRIT, 2: CRIT, 3: CRIT, 4: WARN, 5: OLIVE, 6: OLIVE}
    COLOR_GRAVEDAD = {"GRAVE": CRIT, "MEDIO": WARN, "LEVE": OLIVE}
    COLOR_LABOR = {"ESTIBADOR": "#2E7BB5" if TEMA == "oscuro" else "#1F5C8B",
                   "CHOFER KIA": "#C97A4E" if TEMA == "oscuro" else "#A0522D",
                   "PACKING": "#87A048" if TEMA == "oscuro" else "#5B6E2F",
                   "SEGURIDAD PATRIMONIAL": "#9A81CC" if TEMA == "oscuro" else "#6B4E9E",
                   "CHOFER BUS": "#4FA8A4" if TEMA == "oscuro" else "#2E7D7A",
                   "CAMPO": "#BBA63C" if TEMA == "oscuro" else "#8C7A1F",
                   "NO DEFINIDO": IDLE}
    EXTRA_MATRIZ[SIN_OBSERVACIONES]["color"] = GOOD
    EXTRA_MATRIZ[SIN_VERIFICAR]["color"] = IDLE
    EXTRA_MATRIZ[SIN_OBSERVACIONES]["fg"] = "#0E1A12" if TEMA == "oscuro" else "#FFFFFF"
    EXTRA_MATRIZ[SIN_VERIFICAR]["fg"] = "#14181C" if TEMA == "oscuro" else "#FFFFFF"


# La paleta viva arranca en claro; el tablero la cambia si el usuario lo pide.
aplicar_tema("claro")
