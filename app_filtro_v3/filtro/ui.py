# -*- coding: utf-8 -*-
"""
Capa visual del tablero: paleta, CSS y componentes HTML.

Reproduce el lenguaje del artefacto — fondo papel, rieles de color a la
izquierda de cada tarjeta, píldoras de veredicto y filas del padrón teñidas
según la decisión. Todo el color vive aquí y en config.py.
"""
from __future__ import annotations
import html as _html

import pandas as pd

from . import config as C
from . import maestro as M

# --------------------------------------------------------------- paleta
# Dos afinaciones del mismo lenguaje visual. aplicar_tema() reescribe estas
# variables del módulo y todo lo que las lea (tarjetas, padrón, leyendas)
# cambia con ellas.
TEMAS = {
    "claro": dict(PAPER="#F4F3EE", SURFACE="#FCFCFA", SURFACE2="#EFEEE8",
                  INK="#14171A", INK2="#4A524E", MUTED="#7C847F",
                  RULE="#DCDBD2", RULE_SOFT="#E9E8E1",
                  SOMBRA="0 1px 2px rgba(20,23,26,.05), 0 6px 20px -14px rgba(20,23,26,.22)",
                  VELO="rgba(255,255,255,.55)", ALFA=1.0),
    "oscuro": dict(PAPER="#11151A", SURFACE="#191F26", SURFACE2="#222A33",
                   INK="#EDF1F4", INK2="#C3CBD3", MUTED="#8E98A2",
                   RULE="#2D3742", RULE_SOFT="#242C35",
                   SOMBRA="0 1px 2px rgba(0,0,0,.5), 0 10px 28px -18px rgba(0,0,0,.9)",
                   VELO="rgba(255,255,255,.035)", ALFA=1.7),
}
TEMA = "claro"
PAPER = TEMAS["claro"]["PAPER"]
SURFACE = TEMAS["claro"]["SURFACE"]
SURFACE2 = TEMAS["claro"]["SURFACE2"]
INK = TEMAS["claro"]["INK"]
INK2 = TEMAS["claro"]["INK2"]
MUTED = TEMAS["claro"]["MUTED"]
RULE = TEMAS["claro"]["RULE"]
RULE_SOFT = TEMAS["claro"]["RULE_SOFT"]
SOMBRA = TEMAS["claro"]["SOMBRA"]
VELO = TEMAS["claro"]["VELO"]
ALFA = 1.0

VEREDICTO_ESTILO: dict = {}
LABOR_ESTILO: dict = {}
TIPO_ESTILO: dict = {}
GRAVEDAD_ESTILO: dict = {}
PRIZE_ESTILO: dict = {}


def _hex_rgb(h: str):
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def tenue(color: str, alfa: float) -> str:
    """Color de acento en versión traslúcida, para fondos de fila y píldoras."""
    r, g, b = _hex_rgb(color)
    return f"rgba({r},{g},{b},{min(alfa * ALFA, .95):.3f})"


def _tono(color: str, factor: float) -> str:
    """Aclara (factor>0) u oscurece (factor<0) un color para texto sobre fondo."""
    r, g, b = _hex_rgb(color)
    if factor >= 0:
        r, g, b = (int(x + (255 - x) * factor) for x in (r, g, b))
    else:
        r, g, b = (int(x * (1 + factor)) for x in (r, g, b))
    return f"#{r:02X}{g:02X}{b:02X}"


def aplicar_tema(nombre: str = "claro") -> None:
    """Fija la paleta de la interfaz. Llamar ANTES de css() y de pintar nada."""
    global TEMA, PAPER, SURFACE, SURFACE2, INK, INK2, MUTED, RULE, RULE_SOFT
    global SOMBRA, VELO, ALFA
    global VEREDICTO_ESTILO, LABOR_ESTILO, TIPO_ESTILO, GRAVEDAD_ESTILO, PRIZE_ESTILO
    TEMA = nombre if nombre in TEMAS else "claro"
    t = TEMAS[TEMA]
    PAPER, SURFACE, SURFACE2 = t["PAPER"], t["SURFACE"], t["SURFACE2"]
    INK, INK2, MUTED = t["INK"], t["INK2"], t["MUTED"]
    RULE, RULE_SOFT, SOMBRA, VELO = t["RULE"], t["RULE_SOFT"], t["SOMBRA"], t["VELO"]
    ALFA = t["ALFA"]
    oscuro = TEMA == "oscuro"
    claro_txt = (lambda c: _tono(c, .35)) if oscuro else (lambda c: _tono(c, -.25))

    def etiqueta(color, alfa=.16):
        """Fondo traslúcido + texto legible, en ambos temas."""
        return dict(bg=tenue(color, alfa), fg=claro_txt(color))

    VEREDICTO_ESTILO = {
        v: dict(fondo=tenue(C.COLOR[v], .13 if v != "APTO CON OBSERVACION" else .07),
                riel=C.COLOR[v],
                pill_bg=(C.COLOR[v] if v in ("NO APTO", "REVISION EN COMITE")
                         else tenue(C.COLOR[v], .18)),
                pill_fg=("#FFFFFF" if v == "NO APTO" else
                         ("#221704" if v == "REVISION EN COMITE" else claro_txt(C.COLOR[v]))))
        for v in C.VEREDICTOS}
    LABOR_ESTILO = {k: etiqueta(v, .18) for k, v in C.COLOR_LABOR.items()}
    TIPO_ESTILO = {"PROPIO": etiqueta(C.GOOD, .16),
                   "TERCERO": etiqueta(C.WARN, .16),
                   "NO DEFINIDO": etiqueta(C.IDLE, .14)}
    GRAVEDAD_ESTILO = {"GRAVE": etiqueta(C.CRIT), "MEDIO": etiqueta(C.WARN),
                       "LEVE": etiqueta(C.OLIVE)}
    PRIZE_ESTILO = {"ACTIVO EN PLANILLA": etiqueta(C.GOOD),
                    "CESADO": etiqueta(C.WARN),
                    "SIN DATO DE VIGENCIA": etiqueta(C.IDLE, .14),
                    "SIN MAESTRO CARGADO": etiqueta(C.IDLE, .14),
                    M.SIN_MATCH: etiqueta(C.CRIT, .17)}


aplicar_tema("claro")


def tight(s: str) -> str:
    """Quita líneas en blanco: una línea vacía corta el bloque HTML en Markdown
    y Streamlit acabaría mostrando el CSS como texto."""
    return "\n".join(l for l in s.splitlines() if l.strip())


def esc(x) -> str:
    if x is None or (isinstance(x, float) and x != x):
        return ""
    return _html.escape(str(x))


def color_indice(v) -> str:
    """Color del índice de riesgo, siguiendo la paleta del tema activo."""
    v = v or 0
    return C.CRIT if v >= 75 else C.WARN if v >= 50 else C.AMBAR2 if v >= 30 else C.GOOD


# ------------------------------------------------------------------ CSS
def css() -> str:
    return tight(f"""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>
:root{{
  --paper:{PAPER}; --surface:{SURFACE}; --surface-2:{SURFACE2}; --ink:{INK}; --ink-2:{INK2};
  --muted:{MUTED}; --rule:{RULE}; --rule-soft:{RULE_SOFT}; --brand:{C.BRAND};
  --crit:{C.CRIT}; --warn:{C.WARN}; --good:{C.GOOD}; --olive:{C.OLIVE}; --idle:{C.IDLE};
  --velo:{VELO}; --shadow:{SOMBRA};
  --brand-suave:{tenue(C.BRAND, .14)}; --crit-suave:{tenue(C.CRIT, .12)};
}}
.stApp, .stApp button, .stApp input, .stApp select, .stApp textarea, .stMarkdown {{
  font-family:"IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
}}
/* los iconos de Streamlit son ligaduras: deben conservar su tipografía */
[data-testid="stIconMaterial"], .material-icons, .material-icons-outlined,
span[class*="material-symbols"], [data-testid="stIconMaterial"] * {{
  font-family:"Material Symbols Rounded","Material Symbols Outlined","Material Icons" !important;
}}
.stApp {{ background:var(--paper); }}
.block-container {{ padding-top:1.1rem; padding-bottom:3.5rem; max-width:1480px; }}
h1,h2,h3,h4 {{ font-family:Archivo, system-ui, sans-serif !important; letter-spacing:-.02em;
  color:var(--ink); }}
h1 {{ font-weight:700 !important; }}
h2 {{ font-weight:600 !important; font-size:1.42rem !important; }}
h3 {{ font-weight:600 !important; font-size:1.12rem !important; }}
.mono {{ font-family:"IBM Plex Mono", ui-monospace, monospace; font-variant-numeric:tabular-nums; }}

/* ---------------- barra lateral ---------------- */
section[data-testid="stSidebar"] {{ background:var(--surface); border-right:1px solid var(--rule); }}
section[data-testid="stSidebar"] .block-container {{ padding-top:1.1rem; }}
section[data-testid="stSidebar"] h3 {{ font-size:1rem !important; }}
section[data-testid="stSidebar"] label {{ font-size:11px !important; font-weight:600 !important;
  letter-spacing:.08em; text-transform:uppercase; color:var(--muted) !important; }}
section[data-testid="stSidebar"] hr {{ margin:.9rem 0; border-color:var(--rule-soft); }}
.ruta {{ font-family:"IBM Plex Mono", monospace; font-size:11px; color:var(--ink-2);
  background:var(--surface-2); border:1px solid var(--rule-soft); border-radius:7px;
  padding:7px 9px; margin:4px 0 8px; word-break:break-all; line-height:1.35; }}

/* ---------------- pestañas ---------------- */
.stTabs [data-baseweb="tab-list"] {{ gap:2px; border-bottom:1px solid var(--rule);
  background:transparent; }}
.stTabs [data-baseweb="tab"] {{ height:40px; padding:0 15px; background:transparent;
  border-radius:8px 8px 0 0; font-weight:500; font-size:13.5px; color:var(--muted); }}
.stTabs [data-baseweb="tab"]:hover {{ background:var(--surface-2); color:var(--ink-2); }}
.stTabs [aria-selected="true"] {{ background:var(--surface) !important; color:var(--brand) !important;
  font-weight:600 !important; border:1px solid var(--rule); border-bottom-color:var(--surface); }}
.stTabs [data-baseweb="tab-highlight"] {{ background:var(--brand); }}

/* ---------------- navegación de secciones (radio con aspecto de pestañas) ---
   Se usa un radio con key en vez de st.tabs para que la sección abierta
   sobreviva a cada filtro; el CSS lo disfraza de pestañas.                  */
.st-key-navseccion div[role="radiogroup"] {{ gap:4px; flex-wrap:wrap; padding:0;
  align-items:flex-end; border-bottom:2px solid var(--rule); }}
.st-key-navseccion div[role="radiogroup"] > div {{ margin:0 !important; }}
.st-key-navseccion [data-testid="stRadioOption"],
.st-key-navseccion label[data-baseweb="radio"] {{ height:40px; padding:0 16px;
  display:flex; align-items:center; margin:0 !important; cursor:pointer;
  background:var(--surface-2); border:1px solid var(--rule); border-bottom:0;
  border-radius:9px 9px 0 0; position:relative; top:2px;
  transition:background .12s ease, color .12s ease; }}
.st-key-navseccion [data-testid="stRadioOption"]:hover,
.st-key-navseccion label[data-baseweb="radio"]:hover {{ background:var(--brand-suave); }}
/* el círculo del radio: primer div dentro del contenido del label */
.st-key-navseccion [data-testid="stRadioOption"] > div > div:first-child,
.st-key-navseccion label[data-baseweb="radio"] > div:first-of-type {{ display:none !important; }}
.st-key-navseccion [data-testid="stRadioOption"] p,
.st-key-navseccion label[data-baseweb="radio"] p {{ font-size:13.5px !important;
  font-weight:600 !important; margin:0 !important; letter-spacing:0 !important;
  text-transform:none !important; color:var(--muted) !important; white-space:nowrap; }}
/* PESTAÑA ACTIVA: fondo de marca, texto en blanco y acento arriba */
.st-key-navseccion [data-testid="stRadioOption"][data-selected="true"],
.st-key-navseccion div[role="radiogroup"] > div:has(input:checked) > label {{
  background:var(--brand) !important; border-color:var(--brand);
  box-shadow:0 -3px 0 0 var(--brand) inset, 0 -2px 10px -6px var(--brand); }}
.st-key-navseccion [data-testid="stRadioOption"][data-selected="true"] p,
.st-key-navseccion div[role="radiogroup"] > div:has(input:checked) p {{
  color:#FFFFFF !important; font-weight:700 !important; }}

/* ---- controles segmentados (Sentido, Desagregar por, Cómo subir) ---- */
[class*="st-key-seg"] div[role="radiogroup"] {{ gap:3px; flex-wrap:wrap; background:var(--surface-2);
  border:1px solid var(--rule); border-radius:10px; padding:3px; }}
[class*="st-key-seg"] div[role="radiogroup"] > div {{ margin:0 !important; }}
[class*="st-key-seg"] [data-testid="stRadioOption"], [class*="st-key-seg"] label[data-baseweb="radio"] {{
  display:flex; align-items:center; height:30px; padding:0 12px; margin:0 !important;
  border-radius:7px; cursor:pointer; }}
[class*="st-key-seg"] [data-testid="stRadioOption"] > div > div:first-child,
[class*="st-key-seg"] label[data-baseweb="radio"] > div:first-of-type {{ display:none !important; }}
[class*="st-key-seg"] [data-testid="stRadioOption"] p, [class*="st-key-seg"] label[data-baseweb="radio"] p {{
  font-size:12px !important; font-weight:600 !important; margin:0 !important;
  text-transform:none !important; letter-spacing:0 !important;
  color:var(--muted) !important; white-space:nowrap; }}
[class*="st-key-seg"] [data-testid="stRadioOption"]:hover {{ background:var(--brand-suave); }}
[class*="st-key-seg"] [data-testid="stRadioOption"][data-selected="true"],
[class*="st-key-seg"] div[role="radiogroup"] > div:has(input:checked) > label {{
  background:var(--brand); box-shadow:var(--shadow); }}
[class*="st-key-seg"] [data-testid="stRadioOption"][data-selected="true"] p,
[class*="st-key-seg"] div[role="radiogroup"] > div:has(input:checked) p {{ color:#FFFFFF !important; }}

/* ---------------- encabezado ---------------- */
.hero {{ display:flex; align-items:flex-start; gap:16px; flex-wrap:wrap;
  padding:2px 0 14px; border-bottom:1px solid var(--rule); margin-bottom:16px;
  border-left:5px solid var(--brand); padding-left:14px; }}
.hero .mark {{ width:38px; height:38px; flex:0 0 38px; align-self:flex-start; margin-top:4px;
  border-radius:9px; background-color:var(--brand);
  background-image:url("data:image/svg+xml;utf8,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%23fff' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M12 3 4.5 6v5.5c0 4.3 3.1 8.3 7.5 9.5 4.4-1.2 7.5-5.2 7.5-9.5V6L12 3Z'/%3E%3Cpath d='m9 12 2 2 4-4'/%3E%3C/svg%3E");
  background-repeat:no-repeat; background-position:center; background-size:20px 20px; }}
.hero h1 {{ font-size:32px; margin:0; line-height:1.1; }}
.hero .heroid {{ min-width:230px; }}
.hero .sub {{ color:var(--muted); font-size:12.5px; margin-top:5px;
  font-family:"IBM Plex Mono", monospace; word-break:break-all; }}
.hero .sub2 {{ color:var(--muted); font-size:11.5px; margin-top:2px; }}

/* --------- totales de cabecera: personas y delitos --------- */
.herotot {{ display:flex; gap:10px; align-self:center; flex-wrap:wrap; }}
.tot {{ background:var(--surface); border:1px solid var(--rule); border-left:5px solid var(--c);
  border-radius:11px; padding:9px 16px 9px 13px; min-width:150px; box-shadow:var(--shadow); }}
.tot .lb {{ font-size:10px; font-weight:700; letter-spacing:.09em; text-transform:uppercase;
  color:var(--c); }}
.tot .vl {{ font-family:Archivo; font-weight:700; font-size:34px; line-height:1.05;
  letter-spacing:-.035em; color:var(--ink); margin-top:3px;
  font-variant-numeric:tabular-nums; }}
.tot .pe {{ font-size:10.5px; color:var(--muted); margin-top:2px; }}
@media(max-width:900px){{ .herotot{{ width:100%; }} .tot{{ flex:1; }} }}
.conf {{ margin-left:auto; align-self:center; display:inline-flex; align-items:center; gap:7px;
  font-size:10.5px; font-weight:700; letter-spacing:.09em; text-transform:uppercase;
  color:var(--crit); background:var(--crit-suave);
  border:1px solid {tenue(C.CRIT, .35)}; padding:6px 12px; border-radius:999px; }}

/* ---------------- franja de planilla ---------------- */
.pstrip {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px;
  margin:2px 0 4px; }}
.pchip {{ background:var(--surface); border:1px solid var(--rule); border-radius:11px;
  padding:9px 13px; border-left:4px solid var(--c); box-shadow:var(--shadow); }}
.pchip .v {{ font-family:Archivo; font-weight:700; font-size:24px; line-height:1.1;
  letter-spacing:-.03em; color:var(--ink); font-variant-numeric:tabular-nums; }}
.pchip .l {{ font-size:10.5px; font-weight:700; letter-spacing:.07em; text-transform:uppercase;
  color:var(--c); margin-top:2px; }}
.pchip .s {{ font-size:11px; color:var(--muted); margin-top:2px; }}

/* ---------------- tarjetas KPI ---------------- */
.kpirow {{ display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin:4px 0 6px; }}
@media(max-width:1100px){{ .kpirow{{ grid-template-columns:repeat(2,1fr); }} }}
.kpi {{ background:var(--surface); border:1px solid var(--rule); border-radius:12px;
  padding:14px 16px 13px; position:relative; overflow:hidden; box-shadow:var(--shadow); }}
.kpi::before {{ content:""; position:absolute; inset:0 auto 0 0; width:5px; background:var(--c); }}
.kpi .lab {{ display:flex; align-items:center; gap:7px; font-size:11px; font-weight:700;
  letter-spacing:.08em; text-transform:uppercase; color:var(--c); }}
.kpi .dot {{ width:8px; height:8px; border-radius:50%; background:currentColor; flex:none; }}
.kpi .big {{ font-family:Archivo; font-weight:700; font-size:42px; line-height:1;
  letter-spacing:-.035em; margin-top:9px; color:var(--ink); }}
.kpi .sub {{ font-size:12px; color:var(--muted); margin-top:5px; line-height:1.35; }}
.kpi .bar {{ height:4px; background:var(--rule-soft); border-radius:3px; margin-top:11px;
  overflow:hidden; }}
.kpi .bar i {{ display:block; height:100%; background:var(--c); border-radius:3px; }}

/* contenedores st.container(border=True) con el mismo aspecto de panel */
div[data-testid="stVerticalBlockBorderWrapper"] {{ background:var(--surface);
  border:1px solid var(--rule) !important; border-radius:12px; box-shadow:var(--shadow);
  padding:14px 16px 12px; }}
div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stVerticalBlockBorderWrapper"] {{
  box-shadow:none; }}
.ptit {{ font-family:Archivo; font-weight:600; font-size:14px; color:var(--ink);
  letter-spacing:-.01em; }}
.psub {{ font-size:12px; color:var(--muted); margin-top:2px; margin-bottom:6px; }}
/* ---------------- panel / sección ---------------- */
.panel {{ background:var(--surface); border:1px solid var(--rule); border-radius:12px;
  padding:14px 16px 16px; box-shadow:var(--shadow); margin-bottom:6px; }}
.panel h4 {{ font-size:14px !important; margin:0 0 3px; font-weight:600 !important; }}
.panel .ph {{ font-size:12px; color:var(--muted); margin-bottom:10px; }}
.sectitle {{ display:flex; align-items:baseline; gap:12px; flex-wrap:wrap; margin:6px 0 10px; }}
.sectitle h2 {{ margin:0; }}
.sectitle p {{ margin:0; color:var(--muted); font-size:13px; }}

/* ---------------- píldoras y etiquetas ---------------- */
.pill {{ display:inline-flex; align-items:center; font-size:10.5px; font-weight:700;
  letter-spacing:.045em; padding:3px 9px; border-radius:999px; white-space:nowrap; }}
.tag {{ display:inline-block; font-size:10.5px; font-weight:700; letter-spacing:.04em;
  padding:2.5px 8px; border-radius:5px; white-space:nowrap; }}
.nchip {{ display:inline-grid; place-items:center; font-family:Archivo; font-weight:700;
  font-size:11.5px; min-width:30px; height:22px; padding:0 7px; border-radius:6px; color:#fff; }}

/* ---------------- fila de métricas dentro de tarjeta ---------------- */
.mini {{ display:grid; grid-template-columns:repeat(3,1fr); gap:10px; margin-top:13px;
  padding-top:12px; border-top:1px solid var(--rule-soft); }}
.mini b {{ font-family:Archivo; font-size:24px; font-weight:700; letter-spacing:-.025em;
  display:block; line-height:1.05; }}
.mini span {{ font-size:10.5px; color:var(--muted); display:block; margin-top:4px; line-height:1.3; }}

/* ---------------- barra apilada horizontal ---------------- */
.stack {{ display:flex; height:26px; border-radius:6px; overflow:hidden; gap:2px; margin-top:10px; }}
.stack i {{ display:block; }}

/* ---------------- lista de señales ---------------- */
.sig {{ display:grid; grid-template-columns:1fr 104px; gap:12px; align-items:center;
  padding:9px 0; border-top:1px solid var(--rule-soft); }}
.sig:first-child {{ border-top:0; }}
.sig .t {{ font-size:12.5px; color:var(--ink-2); line-height:1.3; }}
.sig .track {{ height:8px; background:var(--rule-soft); border-radius:5px; margin-top:6px;
  overflow:hidden; }}
.sig .track i {{ display:block; height:100%; border-radius:5px; }}
.sig .n {{ text-align:right; font-size:11px; color:var(--muted); }}
.sig .n b {{ font-family:Archivo; font-size:21px; color:var(--ink); letter-spacing:-.02em;
  display:block; line-height:1.1; }}

/* --------- señales agrupadas por base de cálculo --------- */
.sgroup {{ border:1px solid var(--rule-soft); border-left:4px solid var(--c);
  border-radius:9px; padding:4px 11px 7px; margin-bottom:11px;
  background:var(--velo); }}
.sgroup .sghd {{ display:flex; align-items:center; gap:7px; flex-wrap:wrap;
  font-size:10.5px; font-weight:700; letter-spacing:.08em; text-transform:uppercase;
  color:var(--c); padding:7px 0 4px; }}
.sgroup .sgdot {{ width:7px; height:7px; border-radius:50%; background:var(--c); flex:none; }}
.sgroup .sgbase {{ margin-left:auto; font-weight:600; letter-spacing:.04em;
  text-transform:none; color:var(--muted); font-family:"IBM Plex Mono", monospace;
  font-size:10px; }}
.sgroup .sig:first-child {{ border-top:1px solid var(--rule-soft); }}

/* ---------------- tarjetas de alerta ---------------- */
.agrid {{ display:grid; grid-template-columns:repeat(3,1fr); gap:10px; margin-top:4px;
  align-items:stretch; }}
@media(max-width:1250px){{ .agrid{{ grid-template-columns:repeat(2,1fr); }} }}
@media(max-width:820px){{ .agrid{{ grid-template-columns:1fr; }} }}
.agrid .acard {{ height:auto; }}
.acount {{ display:inline-flex; align-items:center; gap:7px; font-size:11px; font-weight:700;
  letter-spacing:.07em; text-transform:uppercase; color:var(--crit);
  background:var(--crit-suave); border:1px solid {tenue(C.CRIT, .30)};
  padding:4px 11px; border-radius:999px; }}
.acard {{ background:var(--surface); border:1px solid var(--rule); border-left:5px solid var(--c);
  border-radius:11px; padding:13px 15px; box-shadow:var(--shadow); height:100%; }}
.acard .score {{ float:right; font-family:Archivo; font-weight:700; font-size:26px;
  letter-spacing:-.03em; color:var(--c); line-height:1; }}
.acard .nm {{ font-family:Archivo; font-weight:600; font-size:14.5px; letter-spacing:-.01em;
  padding-right:42px; }}
.acard .meta {{ font-size:11.5px; color:var(--muted); margin-top:3px;
  font-family:"IBM Plex Mono", monospace; }}
.acard ul {{ margin:10px 0 0; padding:0; list-style:none; }}
.acard li {{ font-size:12px; color:var(--ink-2); line-height:1.4; padding-left:13px;
  position:relative; margin-bottom:4px; }}
.acard li::before {{ content:""; position:absolute; left:0; top:6.5px; width:5px; height:5px;
  border-radius:50%; background:var(--c); }}
.acard .chips {{ margin-top:10px; display:flex; gap:5px; flex-wrap:wrap; }}

/* ---------------- tarjeta de nivel ---------------- */
.lvl {{ background:var(--surface); border:1px solid var(--rule); border-top:4px solid var(--c);
  border-radius:10px; padding:12px 13px 13px; box-shadow:var(--shadow); height:100%; }}
.lvl .hd {{ display:flex; align-items:center; gap:9px; }}
.lvl .cnt {{ font-size:10.5px; font-weight:600; letter-spacing:.07em; text-transform:uppercase;
  color:var(--muted); }}
.lvl .ti {{ font-weight:600; font-size:13px; margin-top:9px; color:var(--ink); line-height:1.3; }}
.lvl .ca {{ font-size:11.5px; color:var(--muted); margin-top:6px; line-height:1.45; }}
.lvl .ac {{ display:inline-block; margin-top:10px; font-size:10px; font-weight:700;
  letter-spacing:.08em; padding:3px 8px; border-radius:5px; background:var(--c); color:var(--fg); }}

/* ---------------- PADRÓN: filas expandibles ---------------- */
.padron {{ border:1px solid var(--rule); border-radius:12px; overflow:hidden;
  background:var(--surface); box-shadow:var(--shadow); }}
.pgrid {{ display:grid;
  grid-template-columns: minmax(150px,1.9fr) 84px 96px 74px minmax(92px,1fr)
                         minmax(96px,1fr) 42px 62px 58px 104px 132px;
  align-items:center; }}
.phead {{ background:var(--surface-2); border-bottom:1px solid var(--rule);
  position:sticky; top:0; z-index:3; }}
.phead > div {{ padding:10px 8px; font-size:9.5px; font-weight:700; letter-spacing:.06em;
  text-transform:uppercase; color:var(--muted); white-space:nowrap; overflow:hidden;
  text-overflow:ellipsis; }}
.pbody {{ max-height:640px; overflow-y:auto; }}
details.prow {{ border-top:1px solid var(--rule-soft); border-left:4px solid var(--riel);
  background:var(--fondo); }}
details.prow:first-child {{ border-top:0; }}
details.prow > summary {{ list-style:none; cursor:pointer; }}
details.prow > summary::-webkit-details-marker {{ display:none; }}
details.prow > summary:hover {{ filter:brightness(.975); }}
details.prow > summary > div {{ padding:9px 8px; font-size:12.5px; color:var(--ink);
  overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
details.prow .nombre {{ font-weight:600; display:flex; align-items:center; gap:7px; }}
details.prow .car {{ display:inline-block; width:0; height:0; flex:none;
  border-left:5px solid var(--muted); border-top:4px solid transparent;
  border-bottom:4px solid transparent; transition:transform .15s; }}
details.prow[open] .car {{ transform:rotate(90deg); }}
details.prow .num {{ font-variant-numeric:tabular-nums; }}
details.prow .ixwrap {{ display:flex; align-items:center; gap:7px; }}
details.prow .ixtrack {{ flex:1; height:6px; background:{tenue(INK, .10)}; border-radius:4px;
  overflow:hidden; min-width:40px; }}
details.prow .ixtrack i {{ display:block; height:100%; border-radius:4px; }}
.pdet {{ padding:4px 14px 15px 14px; background:var(--velo);
  border-top:1px dashed var(--rule); }}
.dtab {{ width:100%; border-collapse:collapse; margin-top:8px; }}
.dtab th {{ text-align:left; font-size:9.5px; font-weight:700; letter-spacing:.08em;
  text-transform:uppercase; color:var(--muted); padding:5px 7px;
  border-bottom:1px solid var(--rule); }}
.dtab td {{ font-size:11.5px; padding:6px 7px; border-top:1px solid var(--rule-soft);
  color:var(--ink-2); vertical-align:top; }}
.dtab td.d {{ color:var(--ink); font-weight:500; max-width:330px; white-space:normal; }}
.idxline {{ margin-top:9px; font-size:11px; color:var(--muted);
  font-family:"IBM Plex Mono", monospace; }}
.plin {{ margin-top:9px; font-size:11.5px; color:var(--ink-2); line-height:1.5;
  border-left:3px solid var(--c); padding:4px 0 4px 9px; }}
.plin b {{ color:var(--c); font-size:10.5px; letter-spacing:.06em; text-transform:uppercase; }}
.plin .pdat {{ display:flex; gap:8px 16px; flex-wrap:wrap; margin-top:3px; }}
.plin .pdat span {{ white-space:nowrap; }}
.plin .pdat i {{ font-style:normal; color:var(--muted); margin-right:5px; font-size:10.5px;
  text-transform:uppercase; letter-spacing:.05em; }}
.vacio {{ padding:30px; text-align:center; color:var(--muted); font-size:13px; }}

/* ---------------- métricas nativas ---------------- */
div[data-testid="stMetric"] {{ background:var(--surface); border:1px solid var(--rule);
  border-radius:10px; padding:10px 13px; box-shadow:var(--shadow); }}
div[data-testid="stMetricValue"] {{ font-family:Archivo; font-weight:700; font-size:27px;
  letter-spacing:-.025em; }}
div[data-testid="stMetricLabel"] {{ font-size:11px; font-weight:600; letter-spacing:.06em;
  text-transform:uppercase; color:var(--muted); }}

/* ---------------- varios ---------------- */
.stAlert {{ border-radius:10px; }}
hr {{ border-color:var(--rule-soft); }}
.leyenda {{ display:flex; gap:14px; flex-wrap:wrap; font-size:11.5px; color:var(--ink-2);
  margin-top:9px; align-items:center; }}
.leyenda span {{ display:inline-flex; align-items:center; gap:6px; }}
.sw {{ width:11px; height:11px; border-radius:3px; flex:none; }}
.nota {{ background:{tenue(C.BRAND, .07)}; border:1px solid {tenue(C.BRAND, .22)};
  border-radius:10px; padding:13px 15px; font-size:12.5px; color:var(--ink-2); line-height:1.55; }}
.nota b {{ color:var(--ink); }}
.nota ul {{ margin:8px 0 0; padding-left:18px; }}
.nota li {{ margin-bottom:5px; }}

/* ---------------- cromo de Streamlit ---------------- */
.stApp, .stApp p, .stApp li, .stApp span, .stApp label, [data-testid="stMarkdownContainer"] {{
  color:var(--ink); }}
header[data-testid="stHeader"] {{ background:transparent; }}
[data-testid="stToolbar"] {{ color:var(--muted); }}
.stButton button, .stDownloadButton button, [data-testid^="stBaseButton"] {{
  border-radius:9px !important; font-weight:600 !important;
  border:1px solid var(--rule) !important; background:var(--surface-2) !important;
  color:var(--ink) !important; transition:background .12s ease, border-color .12s ease; }}
.stButton button p, .stDownloadButton button p, [data-testid^="stBaseButton"] p,
.stButton button div, .stDownloadButton button div {{ color:inherit !important; }}
.stButton button:hover, .stDownloadButton button:hover,
[data-testid^="stBaseButton"]:hover {{
  border-color:var(--brand) !important; background:var(--brand-suave) !important; }}
.stButton button[kind="primary"], .stDownloadButton button[kind="primary"],
[data-testid="stBaseButton-primary"], [data-testid="stBaseButton-primaryFormSubmit"] {{
  background:var(--brand) !important; border-color:var(--brand) !important;
  color:#FFFFFF !important; }}
.stButton button[kind="primary"] p, .stDownloadButton button[kind="primary"] p,
[data-testid="stBaseButton-primary"] p {{ color:#FFFFFF !important; }}
.stButton button[kind="primary"]:hover, .stDownloadButton button[kind="primary"]:hover,
[data-testid="stBaseButton-primary"]:hover {{ filter:brightness(1.1); }}
.stButton button:disabled, [data-testid^="stBaseButton"]:disabled {{ opacity:.45; }}
[data-baseweb="input"], [data-baseweb="select"] > div, [data-baseweb="base-input"],
.stTextInput input, .stNumberInput input, [data-testid="stTextInputRootElement"] {{
  background:var(--surface) !important; color:var(--ink) !important;
  border-color:var(--rule) !important; }}
[data-baseweb="tag"] {{ background:var(--brand) !important; color:#FFFFFF !important; }}
[data-baseweb="popover"] li, [data-baseweb="menu"] {{ background:var(--surface) !important;
  color:var(--ink) !important; }}
[data-testid="stExpander"] {{ border:1px solid var(--rule); border-radius:11px;
  background:var(--surface); box-shadow:var(--shadow); overflow:hidden; }}
[data-testid="stExpander"] summary {{ color:var(--ink); font-weight:600; }}
[data-testid="stFileUploaderDropzone"], [data-testid="stFileUploader"] section {{
  background:var(--surface-2) !important; border:1.5px dashed var(--rule) !important;
  border-radius:11px; color:var(--ink) !important; }}
[data-testid="stFileUploaderDropzone"]:hover {{ border-color:var(--brand) !important; }}
[data-testid="stNotification"], .stAlert {{ background:var(--surface) !important;
  color:var(--ink) !important; border:1px solid var(--rule); border-radius:11px; }}
[data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] p {{
  color:var(--muted) !important; }}
code, .stCode, pre {{ background:var(--surface-2) !important; color:var(--ink) !important;
  border-radius:8px; }}
[data-testid="stSlider"] [data-baseweb="slider"] div[role="slider"] {{ background:var(--brand); }}
[data-testid="stCheckbox"] svg, [data-testid="stToggle"] svg {{ color:var(--brand); }}
/* las tablas nativas se dibujan en canvas: se enmarcan como tarjeta propia */
[data-testid="stDataFrame"], [data-testid="stDataFrameResizable"] {{
  border-radius:10px; border:1px solid var(--rule); overflow:hidden; }}
</style>
""")


# ------------------------------------------------------------ componentes
def num(v) -> str:
    """12345 -> «12 345». Espacio fino: no se confunde con un decimal."""
    try:
        return f"{int(v):,}".replace(",", " ")
    except (TypeError, ValueError):
        return str(v)


def hero(raiz: str, meta: dict, n_personas: int = 0, n_delitos: int = 0,
         tot_personas: int | None = None, tot_delitos: int | None = None) -> str:
    """
    Encabezado con los dos totales que pide el comité: personas y delitos.
    Si la vista está filtrada, debajo del número grande se indica sobre cuántos
    del total se está mirando.
    """
    def _pie(valor, total, unidad):
        if total is None or int(total) == int(valor):
            return f"{unidad} en la carpeta leída"
        return f"de {num(total)} · vista filtrada"

    return f"""<div class="hero">
  <div class="heroid"><h1>Filtro de antecedentes</h1>
    <div class="sub">{esc(raiz)}</div>
    <div class="sub2">{len(meta['carpetas'])} cuadrillas · {meta['n_excels']} archivos Excel · {meta['n_pdfs']} reportes PDF</div></div>
  <div class="herotot">
    <div class="tot" style="--c:{C.BRAND}">
      <div class="lb">Total de personas</div>
      <div class="vl">{num(n_personas)}</div>
      <div class="pe">{esc(_pie(n_personas, tot_personas, 'colaboradores'))}</div></div>
    <div class="tot" style="--c:{C.CRIT}">
      <div class="lb">Total de delitos</div>
      <div class="vl">{num(n_delitos)}</div>
      <div class="pe">{esc(_pie(n_delitos, tot_delitos, 'registros en rojo'))}</div></div>
  </div>
  <span class="conf">Confidencial · uso interno del comité</span>
</div>"""


def hero_vacio() -> str:
    """Encabezado de la versión web mientras no se ha subido ninguna carpeta."""
    return """<div class="hero">
  <div class="heroid"><h1>Filtro de antecedentes</h1>
    <div class="sub2">Prize · Aquanqa S.A.C. — versión web</div></div>
  <span class="conf">Confidencial · uso interno del comité</span>
</div>"""


def kpi_row(items) -> str:
    """items: [(etiqueta, valor, total, color, subtitulo), ...]"""
    out = []
    for lab, val, tot, color, sub in items:
        p = round(val / tot * 100) if tot else 0
        out.append(f"""<div class="kpi" style="--c:{color}">
  <div class="lab"><span class="dot"></span>{esc(lab)}</div>
  <div class="big">{val}</div>
  <div class="sub">{p}% del grupo · {esc(sub)}</div>
  <div class="bar"><i style="width:{p}%"></i></div></div>""")
    return '<div class="kpirow">' + "".join(out) + '</div>'


def tira_planilla(items) -> str:
    """Franja compacta de datos de planilla: [(etiqueta, valor, sub, color), ...]"""
    piezas = "".join(f"""<div class="pchip" style="--c:{col}">
  <div class="v">{num(val)}</div>
  <div class="l">{esc(lab)}</div>
  <div class="s">{esc(sub)}</div></div>""" for lab, val, sub, col in items)
    return f'<div class="pstrip">{piezas}</div>'


def pill(veredicto: str) -> str:
    e = VEREDICTO_ESTILO.get(veredicto, VEREDICTO_ESTILO["PENDIENTE DE REPORTE"])
    return f'<span class="pill" style="background:{e["pill_bg"]};color:{e["pill_fg"]}">{esc(veredicto)}</span>'


def tag(texto: str, mapa: dict) -> str:
    e = mapa.get(texto, dict(bg="#EDECE6", fg="#6B716C"))
    return f'<span class="tag" style="background:{e["bg"]};color:{e["fg"]}">{esc(texto)}</span>'


def nchip(nivel) -> str:
    try:
        if nivel is None or pd.isna(nivel):
            return '<span style="color:#9AA09B">—</span>'
    except (TypeError, ValueError):
        return '<span style="color:#9AA09B">—</span>'
    n = int(nivel)
    if n not in C.COLOR_NIVEL:
        return '<span style="color:#9AA09B">—</span>'
    fg = "#221704" if n == 4 else "#fff"
    return f'<span class="nchip" style="background:{C.COLOR_NIVEL[n]};color:{fg}">N{n}</span>'


def señales(filas) -> str:
    """filas: [(texto, valor, total, unidad, color), ...]"""
    out = []
    for t, v, tot, u, col in filas:
        p = round(v / tot * 100) if tot else 0
        out.append(f"""<div class="sig"><div>
  <div class="t">{esc(t)}</div><div class="track"><i style="width:{p}%;background:{col}"></i></div></div>
  <div class="n"><b>{num(v)}</b>{p}% de {num(tot)} {esc(u)}</div></div>""")
    return "".join(out)


def bloque_señales(titulo: str, base: int, unidad: str, filas, color_base: str) -> str:
    """
    Un bloque de señales con UNA sola base de cálculo, dicha en el encabezado.

    Mezclar porcentajes sobre personas y sobre delitos en la misma lista hace
    que las cifras no cierren al 100%: aquí cada bloque declara su denominador.
    filas: [(texto, valor, color), ...] — el total es siempre `base`.
    """
    cuerpo = señales([(t, v, base, unidad, col) for t, v, col in filas])
    return f"""<div class="sgroup" style="--c:{color_base}">
  <div class="sghd"><span class="sgdot"></span>{esc(titulo)}
    <span class="sgbase">base: {num(base)} {esc(unidad)}</span></div>
  {cuerpo}</div>"""


# ------------------------------------------------- tarjetas de acción inmediata
def motivos_de(r, df_d: pd.DataFrame) -> list[str]:
    """Por qué esta persona está en la lista de acción inmediata."""
    m = []
    if r['GRAVEDAD_MAX'] == 'GRAVE' and len(df_d):
        gs = df_d[(df_d['DNI'] == r['DNI']) & (df_d['GRAVEDAD'] == 'GRAVE')]['DELITO'].head(2)
        if len(gs):
            m.append("Gravedad alta: " + ", ".join(x.split(' (')[0].lower() for x in gs))
    if pd.notna(r['ANIO_MAX']) and r['ANIO_MAX'] >= C.ANIO_VIGENCIA:
        m.append(f"Registro vigente de {int(r['ANIO_MAX'])}")
    if r['CASOS_ACTIVOS']:
        m.append(f"{int(r['CASOS_ACTIVOS'])} caso(s) sin archivar")
    if r['N_DELITOS'] >= 2:
        m.append(f"{int(r['N_DELITOS'])} delitos acumulados")
    if r['N_JURISDICCIONES'] > 1:
        m.append(f"Antecedentes en {int(r['N_JURISDICCIONES'])} jurisdicciones: "
                 + str(r['JURISDICCIONES']))
    return m


def tarjeta_accion(r, df_d: pd.DataFrame) -> str:
    try:
        niv = int(r['NIVEL_NUM'])
    except (TypeError, ValueError):
        niv = 0
    cn = C.COLOR_NIVEL.get(niv, C.IDLE)
    fg = '#221704' if niv == 4 else '#fff'
    motivos = motivos_de(r, df_d)
    return f"""<div class="acard" style="--c:{cn}">
  <div class="score">{int(r['INDICE'])}</div>
  <div class="nm">{esc(r[C.COL_NOMBRE])}</div>
  <div class="meta">{esc(r['DNI'])} · {esc(r['CARPETAS'])}</div>
  <ul>{"".join(f"<li>{esc(m)}</li>" for m in motivos[:4])}</ul>
  <div class="chips">{nchip(r['NIVEL_NUM'])}
    <span class="tag" style="background:{cn};color:{fg}">{esc(r['ACCION_MATRIZ'])}</span>
    {tag(r['LABOR'], LABOR_ESTILO)}{tag(r['TIPO_PERSONAL'], TIPO_ESTILO)}</div></div>"""


def tarjetas_accion(df_p: pd.DataFrame, df_d: pd.DataFrame) -> str:
    """Rejilla con TODAS las tarjetas que se le pasen — sin recorte a 6."""
    if not len(df_p):
        return ""
    return ('<div class="agrid">'
            + "".join(tarjeta_accion(r, df_d) for _, r in df_p.iterrows())
            + '</div>')


def stack_bar(segmentos) -> str:
    """segmentos: [(valor, color, titulo), ...]"""
    tot = sum(s[0] for s in segmentos) or 1
    piezas = "".join(f'<i style="flex:{v};background:{c}" title="{esc(t)}"></i>'
                     for v, c, t in segmentos if v)
    return f'<div class="stack">{piezas}</div>'


def leyenda_veredictos() -> str:
    s = "".join(f'<span><i class="sw" style="background:{C.COLOR[v]}"></i>'
                f'{esc(v[0] + v[1:].lower())}</span>' for v in C.VEREDICTOS)
    return f'<div class="leyenda">{s}</div>'


# ------------------------------------------------------------ el padrón
CAB = ["Colaborador", "DNI", "Labor", "Personal", "Cuadrilla", "Planilla Prize", "Nv",
       "Delitos", "Último", "Índice", "Veredicto"]


def chip_prize(r) -> str:
    """Insignia compacta con la situación de la persona dentro de Prize."""
    estado = str(r.get('ESTADO_PRIZE') or '')
    if not estado or estado == 'SIN MAESTRO CARGADO':
        return '<span style="color:#9AA09B;font-size:11px">—</span>'
    corto = {'ACTIVO EN PLANILLA': 'ACTIVO', 'CESADO': 'CESADO',
             M.SIN_MATCH: 'NO PRIZE', 'SIN DATO DE VIGENCIA': 'SIN DATO'}.get(estado, estado)
    e = PRIZE_ESTILO.get(estado, dict(bg="#EDECE6", fg="#6B716C"))
    area = str(r.get('AREA') or '')
    titulo = esc(f"{estado} · {area}" if area and area != M.SIN_MATCH else estado)
    return (f'<span class="tag" style="background:{e["bg"]};color:{e["fg"]}" '
            f'title="{titulo}">{esc(corto)}</span>')


def linea_planilla(r) -> str:
    """Los datos de planilla de una persona, dentro de su ficha del padrón."""
    estado = str(r.get('ESTADO_PRIZE') or '')
    if not estado or estado == 'SIN MAESTRO CARGADO':
        return ""
    if estado == M.SIN_MATCH:
        return (f'<div class="plin" style="--c:{C.CRIT}"><b>{esc(M.SIN_MATCH)}</b> — '
                'este DNI no figura en el maestro de funcionarios: probablemente es '
                'personal de contrata o el DNI está mal escrito en el Excel.</div>')
    campos = [("Empresa", r.get('EMPRESA')), ("Código", r.get('COD_FUNCIONARIO')),
              ("Área", r.get('AREA')), ("Cargo", r.get('CARGO')),
              ("Centro de costo", r.get('CENTRO_COSTO')), ("Régimen", r.get('REGIMEN')),
              ("Planilla", r.get('PLANILLA')), ("Ingreso", r.get('FECHA_INGRESO')),
              ("Cese", r.get('FECHA_CESE'))]
    try:
        ant = float(r.get('ANTIGUEDAD_ANIOS'))
        campos.append(("Antigüedad", f"{ant:.1f} años"))
    except (TypeError, ValueError):
        pass
    try:
        if float(r.get('N_CONTRATOS')) > 1:
            campos.append(("Contratos", int(float(r.get('N_CONTRATOS')))))
    except (TypeError, ValueError):
        pass
    piezas = "".join(f"<span><i>{esc(k)}</i>{esc(v)}</span>"
                     for k, v in campos if str(v or "").strip())
    color = C.GOOD if estado == 'ACTIVO EN PLANILLA' else C.WARN
    return (f'<div class="plin" style="--c:{color}"><b>{esc(estado)}</b>'
            f'<span class="pdat">{piezas}</span></div>')


def padron(df_p: pd.DataFrame, df_d: pd.DataFrame, limite: int = 400) -> str:
    if not len(df_p):
        return '<div class="padron"><div class="vacio">Sin resultados con esos filtros.</div></div>'

    cab = "".join(f"<div>{esc(c)}</div>" for c in CAB)
    filas = []
    for _, r in df_p.head(limite).iterrows():
        e = VEREDICTO_ESTILO.get(r['VEREDICTO'], VEREDICTO_ESTILO["PENDIENTE DE REPORTE"])
        tiene = r['N_DELITOS'] > 0
        ix = int(r['INDICE'])
        anio = "—" if pd.isna(r['ANIO_MAX']) else int(r['ANIO_MAX'])
        anio_col = C.CRIT if (not pd.isna(r['ANIO_MAX']) and r['ANIO_MAX'] >= C.ANIO_VIGENCIA) else INK2
        car = '<span class="car"></span>' if tiene else '<span style="width:5px;flex:none"></span>'
        ndel = int(r['N_DELITOS']) if r['TIENE_PDF'] else '<span style="color:#9AA09B">s/rep.</span>'

        resumen = f"""<summary><div class="pgrid">
  <div class="nombre">{car}{esc(r[C.COL_NOMBRE])}</div>
  <div class="mono">{esc(r['DNI'])}</div>
  <div>{tag(r['LABOR'], LABOR_ESTILO)}</div>
  <div>{tag(r['TIPO_PERSONAL'], TIPO_ESTILO)}</div>
  <div style="color:{MUTED};font-size:11.5px">{esc(r['CARPETAS'])}</div>
  <div>{chip_prize(r)}</div>
  <div>{nchip(r['NIVEL_NUM'])}</div>
  <div class="num">{ndel}</div>
  <div class="num" style="color:{anio_col};font-weight:600">{anio}</div>
  <div><span class="ixwrap"><span class="ixtrack"><i style="width:{ix}%;background:{color_indice(ix)}"></i></span>
    <b class="num" style="font-size:12px;color:{INK2}">{ix}</b></span></div>
  <div>{pill(r['VEREDICTO'])}</div>
</div></summary>"""

        if tiene:
            sub = df_d[df_d['DNI'] == r['DNI']]
            cuerpo = "".join(f"""<tr>
  <td>{nchip(d['NIVEL'])}</td>
  <td class="d">{esc(d['DELITO'])}</td>
  <td>{esc(d['CATEGORIA'])}</td>
  <td>{tag(d['GRAVEDAD'], GRAVEDAD_ESTILO)}</td>
  <td class="mono">{'—' if pd.isna(d['ANIO']) else int(d['ANIO'])}</td>
  <td>{tag(d['ESTADO'], {d['ESTADO']: GRAVEDAD_ESTILO['GRAVE']}) if d['ACTIVO'] else '<span style="color:' + MUTED + '">' + esc(d['ESTADO']) + '</span>'}</td>
  <td>{esc(d['JURISDICCION']) or '—'}</td>
  <td>{esc(d['PARTE']) or '—'}</td>
  <td style="color:{MUTED}">{esc(d['FUENTE'])}</td>
  <td class="mono">{esc(d['CASO']) or '—'}</td></tr>""" for _, d in sub.iterrows())
            detalle = f"""<div class="pdet"><table class="dtab">
  <thead><tr><th>Nv</th><th>Delito</th><th>Categoría</th><th>Gravedad</th><th>Año</th>
  <th>Estado</th><th>Jurisdicción</th><th>Parte</th><th>Fuente</th><th>Caso</th></tr></thead>
  <tbody>{cuerpo}</tbody></table>
  {linea_planilla(r)}
  <div class="idxline">Índice {ix} = gravedad {int(r['IDX_GRAVEDAD'])} + vigencia {int(r['IDX_VIGENCIA'])}
  + reincidencia {int(r['IDX_REINCIDENCIA'])} + caso activo {int(r['IDX_CASO_ACTIVO'])}
  + dispersión {int(r['IDX_DISPERSION'])}
  &nbsp;·&nbsp; {int(r['INCID_POLICIALES'])} incidencias policiales (Detalle 1)
  {('&nbsp;·&nbsp; provincia ' + esc(r['PROVINCIA'])) if r['PROVINCIA'] else ''}</div></div>"""
        else:
            detalle = ""

        filas.append(f'<details class="prow" style="--riel:{e["riel"]};--fondo:{e["fondo"]}">'
                     + resumen + detalle + '</details>')

    aviso = ""
    if len(df_p) > limite:
        aviso = (f'<div class="vacio">Se muestran las primeras {limite} de {len(df_p)} filas. '
                 f'Afina los filtros o descarga el CSV para ver el resto.</div>')
    return ('<div class="padron"><div class="pgrid phead">' + cab + '</div><div class="pbody">'
            + "".join(filas) + aviso + '</div></div>')
