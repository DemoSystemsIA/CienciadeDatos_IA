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

# --------------------------------------------------------------- paleta
PAPER = "#F4F3EE"
SURFACE = "#FCFCFA"
SURFACE2 = "#EFEEE8"
INK = "#14171A"
INK2 = "#4A524E"
MUTED = "#7C847F"
RULE = "#DCDBD2"
RULE_SOFT = "#E9E8E1"

# tinte de fila y riel por veredicto  (fondo, riel, texto de la píldora, fondo píldora)
VEREDICTO_ESTILO = {
    "NO APTO":              dict(fondo="rgba(192,51,46,.13)",  riel="#C0332E",
                                 pill_bg="#C0332E", pill_fg="#FFFFFF"),
    "REVISION EN COMITE":   dict(fondo="rgba(232,163,23,.16)", riel="#E8A317",
                                 pill_bg="#E8A317", pill_fg="#221704"),
    "APTO CON OBSERVACION": dict(fondo="rgba(232,163,23,.07)", riel="#C07C11",
                                 pill_bg="rgba(232,163,23,.20)", pill_fg="#8A5A05"),
    "APTO":                 dict(fondo="rgba(31,122,61,.09)",  riel="#1F7A3D",
                                 pill_bg="rgba(31,122,61,.16)", pill_fg="#166030"),
    "PENDIENTE DE REPORTE": dict(fondo="rgba(138,143,139,.12)", riel="#8A8F8B",
                                 pill_bg="rgba(138,143,139,.18)", pill_fg="#565B57"),
}
LABOR_ESTILO = {
    "ESTIBADOR":             dict(bg="#DCE9F4", fg="#164C77"),
    "CHOFER KIA":            dict(bg="#F6E3D8", fg="#8A421F"),
    "PACKING":               dict(bg="#E7EDD9", fg="#4A5C25"),
    "SEGURIDAD PATRIMONIAL": dict(bg="#E7E0F4", fg="#4A3175"),
    "CHOFER BUS":            dict(bg="#D8EDEC", fg="#1C5E5C"),
    "CAMPO":                 dict(bg="#F1EBD2", fg="#6B5A10"),
    "NO DEFINIDO":           dict(bg="#EDECE6", fg="#6B716C"),
}
TIPO_ESTILO = {
    "PROPIO":  dict(bg="#E1F0E5", fg="#1B6B36"),
    "TERCERO": dict(bg="#FCEFD3", fg="#8A5A05"),
    "NO DEFINIDO": dict(bg="#EDECE6", fg="#6B716C"),
}
GRAVEDAD_ESTILO = {
    "GRAVE": dict(bg="rgba(192,51,46,.14)", fg="#A42B26"),
    "MEDIO": dict(bg="rgba(232,163,23,.16)", fg="#8A5A05"),
    "LEVE":  dict(bg="rgba(126,140,51,.16)", fg="#556019"),
}


def tight(s: str) -> str:
    """Quita líneas en blanco: una línea vacía corta el bloque HTML en Markdown
    y Streamlit acabaría mostrando el CSS como texto."""
    return "\n".join(l for l in s.splitlines() if l.strip())


def esc(x) -> str:
    if x is None or (isinstance(x, float) and x != x):
        return ""
    return _html.escape(str(x))


def color_indice(v) -> str:
    v = v or 0
    return "#C0332E" if v >= 75 else "#E8A317" if v >= 50 else "#C07C11" if v >= 30 else "#1F7A3D"


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
  --crit:{C.CRIT}; --warn:{C.WARN}; --good:{C.GOOD}; --olive:#7E8C33; --idle:{C.IDLE};
  --shadow:0 1px 2px rgba(20,23,26,.05), 0 6px 20px -14px rgba(20,23,26,.22);
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
.st-key-navseccion div[role="radiogroup"] {{ gap:2px; flex-wrap:wrap; padding:0;
  align-items:flex-end; border-bottom:1px solid var(--rule); }}
.st-key-navseccion div[role="radiogroup"] > div {{ margin:0 !important; }}
.st-key-navseccion [data-testid="stRadioOption"],
.st-key-navseccion label[data-baseweb="radio"] {{ height:40px; padding:0 15px;
  display:flex; align-items:center; margin:0 !important; cursor:pointer;
  background:transparent; border:1px solid transparent; border-bottom:0;
  border-radius:8px 8px 0 0; position:relative; top:1px; }}
.st-key-navseccion [data-testid="stRadioOption"]:hover,
.st-key-navseccion label[data-baseweb="radio"]:hover {{ background:var(--surface-2); }}
/* el círculo del radio: primer div dentro del contenido del label */
.st-key-navseccion [data-testid="stRadioOption"] > div > div:first-child,
.st-key-navseccion label[data-baseweb="radio"] > div:first-of-type {{ display:none !important; }}
.st-key-navseccion [data-testid="stRadioOption"] p,
.st-key-navseccion label[data-baseweb="radio"] p {{ font-size:13.5px !important;
  font-weight:500 !important; margin:0 !important; letter-spacing:0 !important;
  text-transform:none !important; color:var(--muted) !important; white-space:nowrap; }}
.st-key-navseccion [data-testid="stRadioOption"][data-selected="true"],
.st-key-navseccion div[role="radiogroup"] > div:has(input:checked) > label {{
  background:var(--surface); border-color:var(--rule); }}
.st-key-navseccion [data-testid="stRadioOption"][data-selected="true"] p,
.st-key-navseccion div[role="radiogroup"] > div:has(input:checked) p {{
  color:var(--brand) !important; font-weight:600 !important; }}

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
  color:var(--crit); background:rgba(192,51,46,.09);
  border:1px solid rgba(192,51,46,.35); padding:6px 12px; border-radius:999px; }}

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
  background:rgba(255,255,255,.55); }}
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
  background:rgba(192,51,46,.09); border:1px solid rgba(192,51,46,.30);
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
  grid-template-columns: minmax(170px,2fr) 88px 102px 82px minmax(104px,1fr) 44px 74px 66px 116px 148px;
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
details.prow .ixtrack {{ flex:1; height:6px; background:rgba(20,23,26,.10); border-radius:4px;
  overflow:hidden; min-width:40px; }}
details.prow .ixtrack i {{ display:block; height:100%; border-radius:4px; }}
.pdet {{ padding:4px 14px 15px 14px; background:rgba(255,255,255,.55);
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
.nota {{ background:rgba(23,86,74,.07); border:1px solid rgba(23,86,74,.22);
  border-radius:10px; padding:13px 15px; font-size:12.5px; color:var(--ink-2); line-height:1.55; }}
.nota b {{ color:var(--ink); }}
.nota ul {{ margin:8px 0 0; padding-left:18px; }}
.nota li {{ margin-bottom:5px; }}
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
CAB = ["Colaborador", "DNI", "Labor", "Personal", "Cuadrilla", "Nv", "Delitos",
       "Último", "Índice", "Veredicto"]


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
  <td>{'<span class="tag" style="background:rgba(192,51,46,.13);color:#A42B26">' + esc(d['ESTADO']) + '</span>' if d['ACTIVO'] else '<span style="color:' + MUTED + '">' + esc(d['ESTADO']) + '</span>'}</td>
  <td>{esc(d['JURISDICCION']) or '—'}</td>
  <td>{esc(d['PARTE']) or '—'}</td>
  <td style="color:{MUTED}">{esc(d['FUENTE'])}</td>
  <td class="mono">{esc(d['CASO']) or '—'}</td></tr>""" for _, d in sub.iterrows())
            detalle = f"""<div class="pdet"><table class="dtab">
  <thead><tr><th>Nv</th><th>Delito</th><th>Categoría</th><th>Gravedad</th><th>Año</th>
  <th>Estado</th><th>Jurisdicción</th><th>Parte</th><th>Fuente</th><th>Caso</th></tr></thead>
  <tbody>{cuerpo}</tbody></table>
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
