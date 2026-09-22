# -*- coding: utf-8 -*-
"""
Filtro de Antecedentes — Prize / Aquanqa
Tablero Streamlit sobre la carpeta C:\\FILTER

    streamlit run app.py
"""
from __future__ import annotations
import os
import uuid
import datetime as dt

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

VERSION = "3.4"


# Streamlit renombró use_container_width -> width en 1.49; soportamos ambos.
def _ver(v):
    try:
        return tuple(int(x) for x in v.split('.')[:2])
    except Exception:
        return (0, 0)


ANCHO = {'width': 'stretch'} if _ver(st.__version__) >= (1, 49) else {'use_container_width': True}

st.set_page_config(page_title="Filtro de Antecedentes · Prize", page_icon="🛡️",
                   layout="wide", initial_sidebar_state="expanded")


# ------------------------------------------------- despliegue consistente
# Los módulos se importan DESPUÉS de set_page_config y dentro de un try: si el
# despliegue quedó a medias (app.py nuevo con filtro/ viejo, o al revés), se
# explica qué archivo falta en vez de reventar con un AttributeError opaco.
def _aviso_despliegue(detalle: str, faltan=()):
    st.error(f"**Despliegue incompleto.** `app.py` es la versión {VERSION}, pero los "
             "módulos de `filtro/` no están al día.")
    if faltan:
        st.markdown("\n".join(f"- `{n}` → versión **{v}**" for n, v in faltan))
    st.code(detalle, language=None)
    st.info("Copia la carpeta **completa** al repositorio — los archivos de `filtro/` "
            "también, no solo `app.py` — y vuelve a desplegar. En Windows puedes usar "
            "`publicar_en_repo.bat`. Si el repositorio tiene `filtro/__pycache__` "
            "versionado, quítalo: `git rm -r --cached filtro/__pycache__`.", icon="🛠️")
    st.stop()


try:
    from filtro import config as C
    from filtro import carpetas, charts, maestro, ui
    from filtro import excel_export, loader
    from filtro.loader import construir, escanear, firma as huella
    from filtro.excel_export import construir_excel
    from filtro.validate import validar
except Exception as _e:                      # módulo ausente o desfasado
    _aviso_despliegue(f"{type(_e).__name__}: {_e}")
# Si se publica app.py sin publicar también los módulos de filtro/ (o al revés),
# el fallo aparece como un AttributeError incomprensible en mitad del arranque.
# Aquí se detecta antes y se dice EXACTAMENTE qué archivo quedó viejo.
def _desfasados() -> list[tuple[str, str]]:
    modulos = [("filtro/config.py", C), ("filtro/ui.py", ui), ("filtro/charts.py", charts),
               ("filtro/maestro.py", maestro), ("filtro/carpetas.py", carpetas),
               ("filtro/loader.py", loader), ("filtro/excel_export.py", excel_export)]
    return [(nombre, getattr(mod, "VERSION", "anterior a 3.2"))
            for nombre, mod in modulos if getattr(mod, "VERSION", None) != VERSION]


_viejos = _desfasados()
if _viejos:
    _aviso_despliegue("Versiones distintas entre app.py y filtro/.", _viejos)


# ------------------------------------------------------------------ tema
# Claro / oscuro con un botón. La paleta vive en config, ui y charts: se les
# aplica ANTES de pintar nada para que todo salga del mismo tema.
if st.session_state.get("tema") not in ("claro", "oscuro"):
    st.session_state["tema"] = "claro"
TEMA = st.session_state["tema"]
C.aplicar_tema(TEMA)
ui.aplicar_tema(TEMA)
charts.aplicar_tema(TEMA)

st.markdown(ui.css(), unsafe_allow_html=True)
H = lambda s: st.markdown(ui.tight(s), unsafe_allow_html=True)   # noqa: E731


def seg(nombre: str):
    """Contenedor con aspecto de control segmentado (pestañas pequeñas)."""
    try:
        return st.container(key=f"seg_{nombre}")
    except TypeError:                      # Streamlit antiguo: sin key
        return st.container()


def selector_de_carpeta_js():
    """
    Convierte el cargador de la barra lateral en un SELECTOR DE CARPETA.

    El navegador no deja leer rutas del disco, pero sí dejar elegir una carpeta
    entera (atributo webkitdirectory) y mandar su contenido. Streamlit no expone
    ese atributo, así que se lo añadimos al input desde el propio navegador.
    """
    components.html("""<script>
const doc = window.parent.document;

// Qué se sube de una carpeta: SOLO el Excel de cada carpeta madre, los PDF de
// su subcarpeta Adjuntos y el export de funcionarios. Nada más: ni .zip, ni
// .py, ni cachés, ni el resultado_final de una corrida anterior.
const RE_EXCEL   = /(^|\/)Resumen_NEW_VIP_[^\/]*\.(xlsx|xlsm)$/i;
const RE_PDF     = /(^|\/)Adjuntos?\/[^\/]+\.pdf$/i;
const RE_MAESTRO = /(^|\/)[^\/]*(funcionario|qbiz|maestro|planilla|personal)[^\/]*\.(csv|xlsx|xlsm)$/i;
const RE_BASURA  = /(^|\/)(\.|~\$|__pycache__\/|\.cache_filtro\/)/;

function sirve(f) {
  const ruta = f.webkitRelativePath || f.name || "";
  if (RE_BASURA.test(ruta)) return false;
  return RE_EXCEL.test(ruta) || RE_PDF.test(ruta) || RE_MAESTRO.test(ruta);
}

function marcar() {
  doc.querySelectorAll('section[data-testid="stSidebar"] input[type="file"]')
     .forEach(function (i) {
        if (!i.hasAttribute('webkitdirectory')) {
          i.setAttribute('webkitdirectory', '');
          i.setAttribute('directory', '');
          i.setAttribute('mozdirectory', '');
          i.setAttribute('multiple', '');
        }
     });
}

// Se filtra en fase de captura: corre ANTES del manejador de Streamlit, así que
// basta con reemplazar la lista de archivos del input y lo descartado no se
// sube nunca.
function filtrar(ev) {
  const inp = ev.target;
  if (!inp || !inp.files || !inp.hasAttribute('webkitdirectory')) return;
  const total = inp.files.length;
  const buenos = Array.from(inp.files).filter(sirve);
  if (buenos.length === total) return;
  try {
    const dt = new DataTransfer();
    buenos.forEach(function (f) { dt.items.add(f); });
    inp.files = dt.files;
    console.log("[filtro] carpeta: " + buenos.length + " de " + total +
                " archivos (se ignoran .zip, .py, cachés y todo lo que no sea " +
                "el Excel de la cuadrilla, los PDF de Adjuntos o el maestro).");
  } catch (e) {
    console.warn("[filtro] no se pudo filtrar la carpeta:", e);
  }
}

marcar();
setInterval(marcar, 400);
if (!doc.__filtroCarpeta) {
  doc.addEventListener("change", filtrar, true);
  doc.__filtroCarpeta = true;
}
</script>""", height=0)


# ------------------------------------------------------------------ datos
@st.cache_data(show_spinner=False)
def cargar(raiz: str, sig: str):
    barra = st.progress(0.0, text="Leyendo reportes PDF…")

    def prog(i, total, nombre):
        barra.progress(i / max(total, 1), text=f"Leyendo reportes PDF… {i}/{total} · {nombre}")

    try:
        return construir(raiz, progreso=prog)
    finally:
        barra.empty()


@st.cache_data(show_spinner=False)
def excel_bytes(raiz: str, sig: str):
    p, d, m = cargar(raiz, sig)
    return construir_excel(p, d, m)


# -------------------------------------------------- elección de la carpeta
def cambiar_raiz(nueva: str) -> None:
    """Cambia la carpeta de trabajo, la guarda en el historial y recarga el tablero."""
    nueva = os.path.normpath(os.path.abspath(os.path.expanduser((nueva or "").strip().strip('"'))))
    if not nueva or nueva == st.session_state.get("raiz_actual"):
        return
    st.session_state["raiz_actual"] = nueva
    st.session_state.pop("sel_reciente", None)
    st.session_state["_aviso_carpeta"] = ""
    if os.path.isdir(nueva):
        carpetas.recordar(nueva)
    st.cache_data.clear()
    st.rerun()


# ¿Escritorio del usuario o servidor web? Un servidor NO puede leer el disco de
# quien lo visita, así que allí la carpeta se sube desde el navegador.
SERVIDOR = carpetas.modo_servidor()

if SERVIDOR:
    carpetas.limpiar_temporales()
    if "token_sesion" not in st.session_state:
        st.session_state["token_sesion"] = uuid.uuid4().hex

if "raiz_actual" not in st.session_state:
    if SERVIDOR:
        st.session_state["raiz_actual"] = ""
    else:
        _rec = carpetas.recientes()
        st.session_state["raiz_actual"] = _rec[0] if _rec else C.CARPETA_POR_DEFECTO


def panel_web() -> str:
    """Barra lateral de la versión web: cada usuario sube SU carpeta."""
    destino = carpetas.carpeta_sesion(st.session_state["token_sesion"])
    H('<div class="ptit">Tu carpeta</div>'
      '<div class="psub">Esta versión corre en un servidor, no en tu PC, así que no '
      'puede abrir tus discos. Elige tu carpeta desde el navegador y se procesa al '
      'momento: lo que subas es solo de esta sesión y se borra sola.</div>')

    with seg("carga"):
        modo = st.radio("Cómo subir",
                        ["📁 Carpeta", "🗜️ .zip", "📄 Archivos"],
                        horizontal=True, key="modo_carga", label_visibility="collapsed")

    def _cargar(firma, fn, etiqueta):
        if st.session_state.get("_firma_carga") == firma:
            return
        with st.spinner("Leyendo tu carpeta…"):
            rep = fn()
        st.session_state["_firma_carga"] = firma
        st.session_state["_rep_carga"] = rep
        st.session_state["etiqueta_origen"] = etiqueta
        st.session_state["raiz_actual"] = rep["raiz"] if rep["ok"] else ""

    if modo.endswith("Carpeta"):
        st.caption("Elige la carpeta de tu PC. Solo se leen **el Excel de cada "
                   "cuadrilla y los PDF de su carpeta Adjuntos** (más el maestro, "
                   "si está): los .zip, .py y cualquier otro archivo se ignoran, "
                   "así la carga es mucho más rápida.")
        fs = st.file_uploader(
            "Elegir carpeta", accept_multiple_files=True, key="up_dir",
            type=["xlsx", "xlsm", "pdf", "csv"], label_visibility="collapsed",
            help="Se abrirá el selector de carpetas del navegador. Elige la carpeta "
                 "que contiene las cuadrillas y acepta el aviso de «subir varios "
                 "archivos». Para leer una carpeta comprimida, cambia al modo 🗜️ .zip.")
        selector_de_carpeta_js()
        if fs:
            # Segunda barrera, por si el navegador no filtró: solo pasan el Excel
            # de la cuadrilla, los PDF y el maestro reconocible por su nombre.
            utiles = [f for f in fs
                      if maestro.es_resumen_cuadrilla(f.name)
                      or maestro.parece_maestro(f.name, estricto=True)
                      or f.name.lower().endswith(".pdf")]
            _zips = sum(1 for f in fs if f.name.lower().endswith(
                (".zip", ".rar", ".7z", ".gz", ".tar")))
            _otros = len(fs) - len(utiles) - _zips
            if _zips:
                st.info(f"{_zips} comprimido(s) de la carpeta se ignoraron. "
                        "Para leer uno, cambia al modo 🗜️ .zip.", icon="🗜️")
            if _otros > 0:
                st.caption(f"{_otros} archivo(s) más se ignoraron por no ser el Excel "
                           "de una cuadrilla, un PDF de Adjuntos ni el maestro.")
            _cargar(("carpeta", tuple(sorted((f.name, f.size) for f in utiles))),
                    lambda: carpetas.guardar_sueltos(utiles, destino, estricto=True),
                    f"carpeta · {len(utiles)} archivos")
    elif modo.endswith(".zip"):
        z = st.file_uploader(
            "Sube tu carpeta comprimida", type=["zip"], key="up_zip",
            help="En Windows: clic derecho sobre la carpeta (la que contiene las "
                 "cuadrillas) → Enviar a → Carpeta comprimida (en zip).")
        if z is not None:
            _cargar(("zip", z.name, z.size),
                    lambda: carpetas.extraer_zip(z, destino), z.name)
    else:
        fs = st.file_uploader(
            "Sube los Excel y los PDF", type=["xlsx", "xlsm", "pdf", "csv"],
            accept_multiple_files=True, key="up_files",
            help="Selecciona los Resumen_NEW_VIP_*.xlsx, los PDF de Adjuntos y, si lo "
                 "tienes, el export de funcionarios. La cuadrilla se deduce del "
                 "nombre de cada Excel.")
        if fs:
            _cargar(("sueltos", tuple(sorted((f.name, f.size) for f in fs))),
                    lambda: carpetas.guardar_sueltos(fs, destino),
                    f"{len(fs)} archivos")

    rep = st.session_state.get("_rep_carga")
    if rep and rep["ok"]:
        st.caption(f"✅ {rep['excels']} Excel · {rep['pdfs']} PDF"
                   + (" · maestro de planilla" if rep.get("maestros") else "")
                   + (f" · {rep['zips']} .zip saltados" if rep.get("zips") else "")
                   + (f" · {rep['ignorados']} ignorados" if rep["ignorados"] else ""))
    elif rep:
        st.error(rep["motivo"])

    if st.session_state.get("raiz_actual"):
        if st.button("🗑️  Borrar mis datos del servidor", **ANCHO, key="btn_borrar"):
            carpetas.borrar_sesion(destino)
            for k in ("raiz_actual", "_firma_carga", "_rep_carga", "etiqueta_origen",
                      "up_zip", "up_files", "up_dir"):
                st.session_state.pop(k, None)
            st.rerun()

    H('<div class="nota" style="margin-top:10px;font-size:11.5px;padding:10px 12px">'
      '<b>Estructura esperada dentro del .zip</b><br>'
      'una carpeta por cuadrilla, cada una con su <code>Resumen_NEW_VIP_*.xlsx</code> '
      'y su subcarpeta <code>Adjuntos</code> con los PDF.</div>')
    return st.session_state.get("raiz_actual", "")


# ---------------------------------------------------------------- sidebar
with st.sidebar:
    _l, _t = st.columns([1, .3])
    with _l:
        H(f'<div style="display:flex;align-items:center;gap:9px;margin-bottom:2px">'
          f'<div style="width:26px;height:26px;border-radius:7px;background:{C.BRAND};'
          'display:grid;place-items:center"><svg width="15" height="15" viewBox="0 0 24 24" '
          'fill="none" stroke="#fff" stroke-width="3" stroke-linecap="round" '
          'stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg></div>'
          '<div><div style="font-family:Archivo;font-weight:700;font-size:14px;line-height:1.1">'
          'Filtro de Antecedentes</div>'
          '<div style="font-size:10px;letter-spacing:.07em;text-transform:uppercase;'
          'color:var(--muted)">Prize · Aquanqa S.A.C.</div></div></div>')
    with _t:
        if st.button("🌙" if TEMA == "claro" else "☀️", key="btn_tema", **ANCHO,
                     help=("Cambiar a modo oscuro" if TEMA == "claro"
                           else "Cambiar a modo claro")):
            st.session_state["tema"] = "oscuro" if TEMA == "claro" else "claro"
            st.rerun()
    st.divider()

    raiz = st.session_state["raiz_actual"]

if SERVIDOR:
    with st.sidebar:
        raiz = panel_web()
else:
  with st.sidebar:
    diag = carpetas.diagnostico(raiz)

    H('<div class="ptit">Carpeta de trabajo</div>')
    H(f'<div class="ruta" title="{ui.esc(raiz)}">{ui.esc(raiz)}</div>')

    if st.button("📂  Examinar carpeta…", **ANCHO, type="primary",
                 help="Abre el explorador de Windows para elegir cualquier carpeta."):
        _ruta, _err = carpetas.elegir_carpeta_nativa(raiz)
        st.session_state["_aviso_carpeta"] = _err or ""
        if _ruta:
            cambiar_raiz(_ruta)
    if st.button("🔄  Recargar esta carpeta", **ANCHO,
                 help="Vuelve a leer la carpeta actual."):
        st.cache_data.clear()
        st.rerun()

    if st.session_state.get("_aviso_carpeta"):
        st.warning(st.session_state["_aviso_carpeta"])

    # ---- qué hay dentro de la carpeta elegida ----
    if diag['es_raiz']:
        st.caption(f"✅ {diag['motivo']}")
    elif diag['existe']:
        st.warning(diag['motivo'])
        if diag['sugerencia']:
            _nom = os.path.basename(diag['sugerencia']) or diag['sugerencia']
            if st.button(f"↪  Usar «{_nom}»", **ANCHO, key="btn_sug"):
                cambiar_raiz(diag['sugerencia'])
    else:
        st.error(diag['motivo'])

    # ---- historial ----
    _recs = [r for r in carpetas.recientes() if r.lower() != raiz.lower()]
    if _recs:
        _sel = st.selectbox(
            "Carpetas recientes", ["__nada__"] + _recs, key="sel_reciente",
            format_func=lambda r: ("Abrir una carpeta reciente…" if r == "__nada__"
                                   else carpetas.etiqueta(r)))
        if _sel != "__nada__":
            cambiar_raiz(_sel)

    # ---- ruta a mano y mantenimiento ----
    with st.expander("Escribir la ruta a mano"):
        _manual = st.text_input(
            "Ruta", value=raiz, label_visibility="collapsed",
            help="Carpeta que CONTIENE una subcarpeta por cuadrilla (ESTIBAS01, "
                 "CHOFERES DE KÍAS, PACKING…), cada una con su Resumen_NEW_VIP_*.xlsx "
                 "y su subcarpeta Adjuntos.")
        if st.button("Usar esta ruta", **ANCHO, key="btn_manual"):
            cambiar_raiz(_manual)
        if st.button("🧹  Limpiar caché de PDFs", **ANCHO, key="btn_cache",
                     help="Borra el caché de PDFs en disco y vuelve a leerlos todos."):
            try:
                os.remove(os.path.join(raiz, ".cache_filtro", "pdfs.pkl"))
            except OSError:
                pass
            st.cache_data.clear()
            st.rerun()
        if _recs and st.button("Borrar historial de carpetas", **ANCHO, key="btn_olvidar"):
            carpetas.olvidar()
            st.rerun()

ARBOL = ("FILTER\\\n├── ESTIBAS01\\\n│   ├── Resumen_NEW_VIP_ESTIBAS01.xlsx\n"
         "│   └── Adjuntos\\\n│       ├── ANEXO_12345678_type2.pdf\n"
         "│       └── ANEXO_87654321_type2.pdf\n"
         "├── CHOFERES DE KIAS\\\n│   ├── Resumen_NEW_VIP_CHOFERES DE KIAS.xlsx\n"
         "│   └── Adjuntos\\\n└── PACKING\\\n    └── …")

# ---- versión web sin carpeta subida todavía: pantalla de bienvenida ----
if SERVIDOR and not raiz:
    H(ui.hero_vacio())
    st.warning("**Esta app es pública.** Cualquiera con el enlace puede abrirla y subir "
               "su propia carpeta. Nadie ve los datos de otra sesión, pero los reportes "
               "que subas viajan al servidor: no subas información que no deba salir de "
               "la empresa sin autorización.", icon="⚠️")
    c1, c2 = st.columns([1.15, 1], gap="large")
    with c1:
        H('<div class="sectitle"><h2>Sube tu carpeta y listo</h2>'
          '<p>El tablero se arma solo: no se guarda nada entre sesiones.</p></div>')
        st.markdown(
            "1. En tu PC, ubica la carpeta que **contiene las cuadrillas** "
            "(la que tiene dentro `ESTIBAS01`, `CHOFERES DE KIAS`, `PACKING`…).\n"
            "2. Clic derecho sobre ella → **Enviar a** → **Carpeta comprimida (en zip)**.\n"
            "3. Arrastra ese `.zip` al recuadro de la **barra lateral**.\n\n"
            "¿Prefieres no comprimir? Cambia a **Archivos sueltos** y selecciona los "
            "`Resumen_NEW_VIP_*.xlsx` junto con todos los PDF: la cuadrilla se deduce "
            "del nombre de cada Excel.")
        st.info(f"Tamaño máximo por archivo: **{C.MAX_SUBIDA_MB} MB**. "
                "Si tu carpeta pesa más, sube una cuadrilla a la vez.", icon="📦")
    with c2:
        H('<div class="sectitle"><h2>Estructura esperada</h2></div>')
        st.code(ARBOL, language=None)
    st.caption("¿Quieres que lea directamente una carpeta de tu disco, sin subir nada? "
               "Eso solo puede hacerlo la versión de escritorio: descarga el proyecto y "
               "ejecútalo con run_windows.bat en tu PC.")
    st.stop()

scan = escanear(raiz)
if not scan['existe']:
    st.markdown(ui.css(), unsafe_allow_html=True)
    st.error(f"No existe la carpeta **{raiz}**.")
    st.info("Elígela con el botón **📂 Examinar carpeta…** de la barra lateral, "
            "o escríbela a mano.")
    st.stop()
if not scan['excels']:
    st.error(f"No se encontró ningún **{C.PATRON_EXCEL}** dentro de las subcarpetas.")
    st.info(("La carpeta subida no tiene la estructura esperada:"
             if SERVIDOR else
             "Usa **📂 Examinar carpeta…** y elige la carpeta que CONTIENE a las cuadrillas:")
            + "\n```\n" + ARBOL + "\n```")
    st.stop()
if not SERVIDOR:
    carpetas.recordar(raiz)

sig = huella(scan)
df_p, df_d, meta = cargar(raiz, sig)

with st.sidebar:
    st.divider()
    H('<div style="font-family:Archivo;font-weight:600;font-size:13px;margin-bottom:6px">'
      'Segmentación</div>')
    labores = sorted(df_p['LABOR'].unique())
    tipos = sorted(df_p['TIPO_PERSONAL'].unique())
    f_lab = st.multiselect("Labor", labores, default=[], placeholder="Todas")
    f_tip = st.multiselect("Tipo de personal", tipos, default=[], placeholder="Todos")
    f_cua = st.multiselect("Cuadrilla", meta['carpetas'], default=[], placeholder="Todas")
    f_ver = st.multiselect("Veredicto", C.VEREDICTOS, default=[], placeholder="Todos")
    f_niv = st.multiselect("Nivel matriz", [f"N{n}" for n in range(1, 7)], default=[],
                           placeholder="Todos")
    # ---- filtros de planilla (solo si hay maestro cruzado) ----
    IM = meta.get('maestro') or {}
    HAY_MAESTRO = bool(IM.get('hay_maestro'))
    f_area, f_emp, f_est = [], [], []
    if HAY_MAESTRO:
        _areas = sorted(a for a in df_p['AREA'].dropna().unique() if str(a).strip())
        _emps = sorted(e for e in df_p['EMPRESA'].dropna().unique() if str(e).strip())
        _ests = sorted(e for e in df_p['ESTADO_PRIZE'].dropna().unique() if str(e).strip())
        f_area = st.multiselect("Área (planilla)", _areas, default=[], placeholder="Todas")
        f_emp = st.multiselect("Empresa", _emps, default=[], placeholder="Todas")
        f_est = st.multiselect("Situación en Prize", _ests, default=[], placeholder="Todas")
    solo_delitos = st.checkbox("Solo con delitos en rojo", value=False)
    solo_fuera = st.checkbox(f"Solo «{maestro.SIN_MATCH.lower()}»", value=False,
                             disabled=not HAY_MAESTRO,
                             help="Personas del padrón que no aparecen en el maestro.")
    busca = st.text_input("Buscar nombre o DNI", "", placeholder="nombre o DNI…")

    # ---- maestro de funcionarios ----
    st.divider()
    H('<div class="ptit">Maestro de planilla</div>')
    if HAY_MAESTRO:
        st.caption(f"✅ {IM.get('archivo')} · {ui.num(IM.get('filas_maestro', 0))} DNI · "
                   f"{IM.get('con_match', 0)} cruzaron, {IM.get('sin_match', 0)} fuera")
    elif IM.get('error'):
        st.error(f"No se pudo leer el maestro. {IM['error']}")
    else:
        st.caption("Sin maestro: súbelo para saber quién está en planilla.")
    with st.expander("Cargar maestro de funcionarios"):
        _mf = st.file_uploader("Export de funcionarios (.csv / .xlsx)",
                               type=["csv", "xlsx", "xlsm"], key="up_maestro",
                               label_visibility="collapsed",
                               help="El export de qbiz con la columna payload en JSON. "
                                    "Se guarda en la carpeta de trabajo y se usa en cada "
                                    "recarga.")
        if _mf is not None:
            _firma_m = (_mf.name, _mf.size)
            if st.session_state.get("_firma_maestro") != _firma_m:
                try:
                    _dst = os.path.join(raiz, ".cache_filtro", maestro.NOMBRE_GUARDADO)
                    os.makedirs(os.path.dirname(_dst), exist_ok=True)
                    with open(_dst, "wb") as _f:
                        _f.write(_mf.getbuffer())
                    st.session_state["_firma_maestro"] = _firma_m
                    st.cache_data.clear()
                    st.rerun()
                except OSError as e:
                    st.error(f"No se pudo guardar: {e}")
        if HAY_MAESTRO and st.button("Quitar maestro", **ANCHO, key="btn_quitar_maestro"):
            try:
                os.remove(os.path.join(raiz, ".cache_filtro", maestro.NOMBRE_GUARDADO))
            except OSError:
                pass
            st.session_state.pop("_firma_maestro", None)
            st.cache_data.clear()
            st.rerun()

    st.divider()
    st.download_button("⬇️  Descargar resultado_final.xlsx", data=excel_bytes(raiz, sig),
                       file_name=C.NOMBRE_SALIDA, **ANCHO, type="primary",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    if not SERVIDOR and st.button("💾  Guardar en la carpeta raíz", **ANCHO):
        destino = os.path.join(raiz, C.NOMBRE_SALIDA)
        try:
            with open(destino, "wb") as f:
                f.write(excel_bytes(raiz, sig))
            st.success(f"Guardado en {destino}")
        except PermissionError:
            st.error("El archivo está abierto en Excel. Ciérralo y reintenta.")
        except OSError as e:
            st.error(f"No se pudo escribir: {e}")
    st.caption(f"v{VERSION} · huella `{sig[:10]}` · {dt.datetime.now():%d/%m/%Y %H:%M}")


# ---------------------------------------------------------------- filtros
def aplicar(df):
    m = pd.Series(True, index=df.index)
    if f_lab:
        m &= df['LABOR'].isin(f_lab)
    if f_tip:
        m &= df['TIPO_PERSONAL'].isin(f_tip)
    if f_cua:
        m &= df['CARPETAS_L'].apply(lambda cs: any(c in f_cua for c in cs))
    if f_ver:
        m &= df['VEREDICTO'].isin(f_ver)
    if f_niv:
        m &= df['NIVEL_MATRIZ'].isin(f_niv)
    if f_area:
        m &= df['AREA'].isin(f_area)
    if f_emp:
        m &= df['EMPRESA'].isin(f_emp)
    if f_est:
        m &= df['ESTADO_PRIZE'].isin(f_est)
    if solo_delitos:
        m &= df['N_DELITOS'] > 0
    if solo_fuera:
        m &= df['EN_PRIZE'] == 'NO'
    if busca.strip():
        q = busca.strip().lower()
        m &= (df[C.COL_NOMBRE].str.lower().str.contains(q, na=False) |
              df['DNI'].astype(str).str.contains(q, na=False))
    return df[m]


P = aplicar(df_p)
D = df_d[df_d['DNI'].isin(set(P['DNI']))] if len(df_d) else df_d
filtrado = bool(f_lab or f_tip or f_cua or f_ver or f_niv or f_area or f_emp or f_est
                or solo_delitos or solo_fuera or busca.strip())

# ------------------------------------------------------------------ hero
ORIGEN = (f"Carpeta subida · {st.session_state.get('etiqueta_origen', 'sin nombre')}"
          if SERVIDOR else meta['raiz'])
H(ui.hero(ORIGEN, meta, len(P), len(D), len(df_p), len(df_d)))
if SERVIDOR:
    st.caption("⚠️ App pública: cada visitante trabaja con su propia carpeta y no ve la "
               "de los demás. Tus archivos se borran del servidor al cerrar la sesión.")
if filtrado:
    st.info(f"Vista filtrada · **{len(P)}** de {len(df_p)} colaboradores y "
            f"**{len(D)}** de {len(df_d)} delitos en rojo. "
            "Los totales de la cabecera siguen a este filtro.")

v = P['VEREDICTO'].value_counts()
H(ui.kpi_row([
    ("Retiro", int(v.get('NO APTO', 0)), len(P), C.CRIT,
     "niveles 1-3: patrimonio, homicidio, delitos sexuales"),
    ("Se estudia salida", int(v.get('REVISION EN COMITE', 0) + v.get('APTO CON OBSERVACION', 0)),
     len(P), C.WARN, "niveles 4-6: autoridad, ebriedad, familia, faltas leves"),
    ("Sin observaciones", int(v.get('APTO', 0)), len(P), C.GOOD,
     "con reporte adjunto y ningún registro en rojo"),
    ("Sin verificar", int(v.get('PENDIENTE DE REPORTE', 0)), len(P), C.IDLE,
     "no tienen PDF en la carpeta Adjuntos")]))

if HAY_MAESTRO:
    _fuera = int((P['EN_PRIZE'] == 'NO').sum())
    H(ui.tira_planilla([
        ("En planilla Prize", int((P['EN_PRIZE'] == 'SI').sum()),
         f"{(P['EN_PRIZE'] == 'SI').mean() * 100:.0f}% del grupo", C.BRAND),
        ("Activos", int((P['ESTADO_PRIZE'] == 'ACTIVO EN PLANILLA').sum()),
         "vigentes en planilla", C.GOOD),
        ("Cesados", int((P['ESTADO_PRIZE'] == 'CESADO').sum()),
         "ya no trabajan aquí", C.WARN),
        (maestro.SIN_MATCH, _fuera,
         "no cruzan con el maestro", C.CRIT),
        ("Áreas distintas", int(P.loc[P['EN_PRIZE'] == 'SI', 'AREA'].nunique()),
         "según planilla", C.IDLE)]))

st.write("")

# ------------------------------------------------- navegación persistente
# Con st.tabs, cualquier filtro (buscar un DNI, marcar una cuadrilla…) hace
# que Streamlit reejecute el script y la vista volvía siempre a «Resumen».
# Guardando la sección en session_state, uno se queda donde estaba.
SECCIONES = ["Resumen", "Planilla Prize", "Propio vs tercero", "Matriz y categorías",
             "Vigencia", "Padrón", "Criterio N1-N6", "Trazabilidad"]
if st.session_state.get("seccion") not in SECCIONES:
    st.session_state["seccion"] = SECCIONES[0]
try:
    _nav = st.container(key="navseccion")
except TypeError:                       # Streamlit antiguo: sin key en container
    _nav = st.container()
with _nav:
    seccion = st.radio("Sección", SECCIONES, key="seccion", horizontal=True,
                       label_visibility="collapsed")
st.write("")

# ================================================================ RESUMEN
if seccion == "Resumen":
    c1, c2 = st.columns([1.35, 1], gap="medium")
    n_retiro = int((P["NIVEL_NUM"].notna() & (P["NIVEL_NUM"] <= 3)).sum())
    n_salida = int((P["NIVEL_NUM"] >= 4).sum())
    n_sinobs = int((P['VEREDICTO'] == 'APTO').sum())
    n_sinver = int((P['VEREDICTO'] == 'PENDIENTE DE REPORTE').sum())

    with c1, st.container(border=True):
        H('<div class="ptit">Personas por nivel de la matriz</div>'
          f'<div class="psub">{n_retiro} para retiro · {n_salida} se estudia salida · '
          f'{n_sinobs} sin observaciones · {n_sinver} sin verificar · '
          f'suman {n_retiro + n_salida + n_sinobs + n_sinver} de {len(P)} personas · '
          'cada persona cuenta una sola vez, en su nivel más crítico</div>')
        st.altair_chart(charts.niveles_matriz(P, D), **ANCHO)
        H('<div class="leyenda">'
          f'<span><i class="sw" style="background:{C.CRIT}"></i>Retiro · N1-N3</span>'
          f'<span><i class="sw" style="background:{C.WARN}"></i>Se estudia salida · N4</span>'
          '<span><i class="sw" style="background:#7E8C33"></i>Se estudia salida · N5-N6</span>'
          f'<span><i class="sw" style="background:{C.GOOD}"></i>Sin observaciones</span>'
          f'<span><i class="sw" style="background:{C.IDLE}"></i>Sin verificar</span></div>')
    with c2, st.container(border=True):
        td, tp = len(D), len(P)
        H('<div class="ptit">Señales de alerta</div>'
          '<div class="psub">Dos bases de cálculo distintas. Los porcentajes de cada bloque '
          'se leen contra su propia base y <b>no se suman entre bloques</b>.</div>'
          + ui.bloque_señales(
              "Sobre los delitos en rojo", td, "delitos", [
                  ("Delitos de gravedad ALTA",
                   int((D['GRAVEDAD'] == 'GRAVE').sum()) if td else 0, C.CRIT),
                  ("Delitos con proceso abierto",
                   int(D['ACTIVO'].sum()) if td else 0, C.CRIT),
                  ("Delitos vigentes 2023 en adelante",
                   int(D['VIGENTE'].sum()) if td else 0, C.WARN),
              ], C.CRIT)
          + ui.bloque_señales(
              "Sobre las personas del padrón", tp, "personas", [
                  ("Personas con al menos un delito en rojo",
                   int((P['N_DELITOS'] > 0).sum()), C.CRIT),
                  ("Personas con caso abierto",
                   int((P['CASOS_ACTIVOS'] > 0).sum()), C.CRIT),
                  ("Personas reincidentes (2 o más)",
                   int((P['N_DELITOS'] >= 2).sum()), C.WARN),
                  ("Antecedentes en 2+ jurisdicciones",
                   int((P['N_JURISDICCIONES'] >= 2).sum()), C.WARN),
                  ("Sin reporte adjunto (punto ciego)",
                   int((~P['TIENE_PDF']).sum()), C.IDLE),
              ], C.BRAND))

    # --- TODOS los rojos críticos: veredicto NO APTO (niveles N1-N3, retiro) ---
    criticos = (P[P['VEREDICTO'] == 'NO APTO']
                .sort_values(['INDICE', 'N_DELITOS', C.COL_NOMBRE],
                             ascending=[False, False, True]))
    H('<div class="sectitle"><h2>Acción inmediata</h2>'
      '<p>Todas las personas en rojo crítico: veredicto <b>NO APTO</b>, niveles N1-N3 '
      '(patrimonio, homicidio, delitos sexuales, extorsión, drogas, armas). '
      'Ordenadas por índice de riesgo.</p>'
      f'<span class="acount">{len(criticos)} de {len(P)} personas para retiro</span></div>')
    if not len(criticos):
        st.info("Ninguna persona en rojo crítico en esta selección.")
    else:
        H(ui.tarjetas_accion(criticos, D))
        cols_cri = ['DNI', C.COL_NOMBRE, 'LABOR', 'TIPO_PERSONAL', 'CARPETAS', 'NIVEL_MATRIZ',
                    'CATEGORIAS', 'N_DELITOS', 'ANIO_MAX', 'CASOS_ACTIVOS', 'JURISDICCIONES',
                    'INDICE', 'VEREDICTO', 'FILTRO']
        st.download_button(f"⬇️  Descargar los {len(criticos)} casos críticos (CSV)",
                           criticos[cols_cri].to_csv(index=False).encode('utf-8-sig'),
                           "rojos_criticos.csv", "text/csv", key="dl_criticos")

        otros = P[(P['N_DELITOS'] > 0) & (P['VEREDICTO'] != 'NO APTO')]
        if len(otros):
            with st.expander(f"Ver también {len(otros)} casos con delitos en rojo que "
                             f"NO son críticos (se estudia salida · N4-N6)"):
                H(ui.tarjetas_accion(
                    otros.sort_values(['INDICE', 'N_DELITOS'], ascending=False), D))

# ====================================================== PROPIO VS TERCERO
elif seccion == "Planilla Prize":
    if not HAY_MAESTRO:
        H('<div class="sectitle"><h2>Planilla Prize</h2>'
          '<p>Cruce del padrón con el maestro de funcionarios, por DNI.</p></div>')
        st.info("Todavía no hay maestro cargado. Súbelo en la barra lateral "
                "(**Planilla Prize → Cargar maestro de funcionarios**) o deja el export "
                "de qbiz dentro de la carpeta de trabajo: el tablero lo detecta solo.",
                icon="📇")
        st.caption("Sirve cualquier .csv/.xlsx con una columna DNI, con los campos sueltos "
                   "o dentro de un JSON en una columna payload.")
    else:
        _fuera = P[P['EN_PRIZE'] == 'NO']
        H('<div class="sectitle"><h2>Quién es cada DNI dentro de Prize</h2>'
          f'<p>Cruce por DNI contra {ui.num(IM.get("filas_maestro", 0))} funcionarios. '
          'Si el DNI no está en el maestro se marca '
          f'<b>{maestro.SIN_MATCH.lower()}</b>: casi siempre es personal de contrata.</p>'
          f'<span class="acount">{len(_fuera)} de {len(P)} no cruzan</span></div>')

        c1, c2 = st.columns([1.25, 1], gap="medium")
        with c1, st.container(border=True):
            H('<div class="ptit">Veredicto por área de planilla</div>'
              '<div class="psub">cada barra es un área; el color, la decisión</div>')
            ch = charts.planilla(P, 'AREA')
            if ch is not None:
                st.altair_chart(ch, **ANCHO)
            else:
                st.info("Sin áreas que mostrar en esta selección.")
        with c2, st.container(border=True):
            H('<div class="ptit">Situación en la empresa</div>'
              '<div class="psub">sobre las ' + ui.num(len(P)) + ' personas filtradas</div>'
              + ui.señales([
                  ("Activos en planilla con delito en rojo",
                   int(((P['ESTADO_PRIZE'] == 'ACTIVO EN PLANILLA') &
                        (P['N_DELITOS'] > 0)).sum()), len(P), "personas", C.CRIT),
                  ("Activos en planilla y NO APTO",
                   int(((P['ESTADO_PRIZE'] == 'ACTIVO EN PLANILLA') &
                        (P['VEREDICTO'] == 'NO APTO')).sum()), len(P), "personas", C.CRIT),
                  ("Cesados (ya no son un riesgo activo)",
                   int((P['ESTADO_PRIZE'] == 'CESADO').sum()), len(P), "personas", C.WARN),
                  (f"{maestro.SIN_MATCH} — revisar contrata",
                   len(_fuera), len(P), "personas", C.WARN),
                  ("Con más de un contrato en el maestro",
                   int((pd.to_numeric(P['N_CONTRATOS'], errors='coerce') > 1).sum()),
                   len(P), "personas", C.IDLE)]))

        st.write("")
        c1, c2 = st.columns(2, gap="medium")
        with c1, st.container(border=True):
            H('<div class="ptit">Cargos más frecuentes</div>'
              '<div class="psub">según el maestro, teñidos por veredicto</div>')
            ch = charts.planilla(P[P['EN_PRIZE'] == 'SI'], 'CARGO', top=10)
            if ch is not None:
                st.altair_chart(ch, **ANCHO)
            else:
                st.info("Sin cargos que mostrar en esta selección.")
        with c2, st.container(border=True):
            H('<div class="ptit">Empresa y régimen</div>'
              '<div class="psub">composición del grupo filtrado</div>')
            _t = (P[P['EN_PRIZE'] == 'SI']
                  .groupby(['EMPRESA', 'TIPO_TRABAJADOR']).size()
                  .reset_index(name='Personas').sort_values('Personas', ascending=False))
            if len(_t):
                st.dataframe(_t, hide_index=True, **ANCHO,
                             height=min(360, 38 * len(_t) + 40))
            else:
                st.info("Nadie del grupo filtrado está en el maestro.")

        st.write("")
        H('<div class="sectitle"><h2>Detalle por persona</h2>'
          '<p>El padrón con los campos que aporta la planilla.</p></div>')
        _cols = ['DNI', C.COL_NOMBRE, 'LABOR', 'ESTADO_PRIZE', 'EMPRESA', 'COD_FUNCIONARIO',
                 'AREA', 'CARGO', 'CENTRO_COSTO', 'REGIMEN', 'TIPO_TRABAJADOR', 'PLANILLA',
                 'FECHA_INGRESO', 'FECHA_CESE', 'ANTIGUEDAD_ANIOS', 'N_CONTRATOS',
                 'N_DELITOS', 'NIVEL_MATRIZ', 'INDICE', 'VEREDICTO']
        _cols = [c for c in _cols if c in P.columns]
        _vis = P.sort_values(['EN_PRIZE', 'INDICE'], ascending=[True, False])[_cols]
        st.dataframe(_vis, hide_index=True, **ANCHO, height=430,
                     column_config={"INDICE": st.column_config.ProgressColumn(
                         "Índice", min_value=0, max_value=100, format="%d"),
                         "ANTIGUEDAD_ANIOS": st.column_config.NumberColumn(
                             "Antigüedad", format="%.1f años")})
        st.download_button("⬇️  Descargar el cruce con planilla (CSV)",
                           _vis.to_csv(index=False).encode('utf-8-sig'),
                           "padron_con_planilla.csv", "text/csv", key="dl_planilla")

elif seccion == "Propio vs tercero":
    H('<div class="sectitle"><h2>La misma lupa sobre cada población</h2>'
      '<p>Las contratas (choferes de kías y de buses) y la planilla propia '
      '(estibadores, packing, seguridad patrimonial, campo) no se comportan igual.</p></div>')
    POR_FILA = 3
    cols, _i = [], 0
    for lb in labores:
        if _i % POR_FILA == 0:
            cols = st.columns(POR_FILA, gap="medium")
        col = cols[_i % POR_FILA]
        _i += 1
        sub = df_p[df_p['LABOR'] == lb]
        subd = df_d[df_d['DNI'].isin(set(sub['DNI']))] if len(df_d) else df_d
        con = sub[sub['N_DELITOS'] > 0]
        tp = sub['TIPO_PERSONAL'].iloc[0]
        segs = [(int((sub['VEREDICTO'] == k).sum()), C.COLOR[k],
                 f"{k}: {int((sub['VEREDICTO'] == k).sum())}") for k in C.VEREDICTOS]
        retiro = (sub['VEREDICTO'] == 'NO APTO').mean() * 100
        sinrep = (~sub['TIENE_PDF']).mean() * 100
        medio = con['INDICE'].mean() if len(con) else 0
        atenua = "" if (not f_lab or lb in f_lab) else "opacity:.45;"
        with col:
            H(f'''<div class="panel" style="{atenua}">
  <div style="display:flex;align-items:center;gap:9px">
    <span class="sw" style="width:13px;height:13px;background:{C.COLOR_LABOR.get(lb, C.IDLE)}"></span>
    <span style="font-family:Archivo;font-weight:700;font-size:17px;letter-spacing:-.02em">{ui.esc(lb)}</span>
    {ui.tag(tp, ui.TIPO_ESTILO)}</div>
  <div class="ph" style="margin:5px 0 0">{len(sub)} personas · {len(subd)} delitos en rojo</div>
  {ui.stack_bar(segs)}
  <div class="mini">
    <div><b style="color:{C.CRIT}">{retiro:.0f}%</b><span>tasa de retiro<br>{int((sub['VEREDICTO'] == 'NO APTO').sum())} de {len(sub)}</span></div>
    <div><b style="color:{C.IDLE}">{sinrep:.0f}%</b><span>sin reporte<br>{int((~sub['TIENE_PDF']).sum())} personas</span></div>
    <div><b style="color:{ui.color_indice(medio)}">{medio:.0f}</b><span>índice medio<br>de {len(con)} con delitos</span></div>
  </div></div>''')
    H(ui.leyenda_veredictos())

    st.write("")
    H('<div class="sectitle"><h2>Composición</h2>'
      '<p>La franja gris es el punto ciego: gente sin reporte adjunto.</p></div>')
    with seg("eje"):
        eje = st.radio("Desagregar por", ["Labor", "Tipo de personal", "Cuadrilla"],
                       horizontal=True, label_visibility="collapsed", key="eje_composicion")
    campo = {"Labor": "LABOR", "Tipo de personal": "TIPO_PERSONAL"}.get(eje)
    if campo is None:
        base = P.explode('CARPETAS_L').rename(columns={'CARPETAS_L': 'CUADRILLA'})
        ch = charts.composicion(base, 'CUADRILLA')
    else:
        ch = charts.composicion(P, campo)
    with st.container(border=True):
        if ch is not None:
            st.altair_chart(ch, **ANCHO)   # el gráfico ya trae su propia leyenda
        else:
            st.info("Sin datos para componer esta vista.")

    st.write("")
    H('<div class="sectitle"><h2>Tabla cruzada</h2><p>Personas por labor y veredicto.</p></div>')
    if len(P):
        cruce = (pd.crosstab(P['LABOR'], P['VEREDICTO'])
                 .reindex(columns=C.VEREDICTOS, fill_value=0))
        cruce['TOTAL'] = cruce.sum(axis=1)
        sty = cruce.style.set_properties(**{'font-weight': '600'})
        for _col in C.VEREDICTOS:
            if _col in cruce.columns:
                sty = sty.apply(ui.degradado, color=C.COLOR[_col], subset=[_col])
        if 'TOTAL' in cruce.columns:
            sty = sty.apply(ui.degradado, color=C.BRAND, subset=['TOTAL'])
        st.dataframe(sty, **ANCHO)

# ===================================================== MATRIZ Y CATEGORÍAS
elif seccion == "Matriz y categorías":
    c1, c2 = st.columns(2, gap="medium")
    with c1:
        H('<div class="sectitle"><h2>Matriz de criticidad</h2>'
          '<p>N1-N6 más las dos categorías sin delitos en rojo.</p></div>')
        filas, suma = [], 0
        for n in range(1, 7):
            pers = int((P['NIVEL_NUM'] == n).sum())
            dels = int((D['NIVEL'] == n).sum()) if len(D) else 0
            suma += pers
            fg = '#221704' if n == 4 else '#fff'
            filas.append(f'''<div class="panel" style="border-left:5px solid {C.COLOR_NIVEL[n]};
  margin-bottom:8px;padding:11px 14px">
  <div style="display:flex;align-items:center;gap:10px">
    {ui.nchip(n)}
    <span style="font-size:12.5px;color:#4A524E;line-height:1.3;flex:1">{ui.esc(C.NIVELES[n][0])}</span>
    <span style="text-align:right;white-space:nowrap">
      <b style="font-family:Archivo;font-size:21px;letter-spacing:-.02em">{pers}</b>
      <span style="font-size:11px;color:#7C847F"> personas · {dels} delitos</span></span></div>
  <span class="tag" style="background:{C.COLOR_NIVEL[n]};color:{fg};margin-top:8px;display:inline-block">
    {ui.esc(C.NIVELES[n][1])}</span></div>''')

        # --- categorías fuera de la matriz: gente sin delitos en rojo ---
        for clave, e in C.EXTRA_MATRIZ.items():
            pers = int((P['VEREDICTO'] == e['veredicto']).sum())
            suma += pers
            filas.append(f'''<div class="panel" style="border-left:5px solid {e['color']};
  margin-bottom:8px;padding:11px 14px">
  <div style="display:flex;align-items:center;gap:10px">
    <span class="nchip" style="background:{e['color']};color:{e['fg']};min-width:74px">{ui.esc(clave)}</span>
    <span style="font-size:12.5px;color:#4A524E;line-height:1.3;flex:1">{ui.esc(e['concepto'])}</span>
    <span style="text-align:right;white-space:nowrap">
      <b style="font-family:Archivo;font-size:21px;letter-spacing:-.02em">{pers}</b>
      <span style="font-size:11px;color:#7C847F"> personas · 0 delitos</span></span></div>
  <span class="tag" style="background:{e['color']};color:{e['fg']};margin-top:8px;display:inline-block">
    {ui.esc(e['accion'])}</span></div>''')

        filas.append(f'''<div class="panel" style="border-left:5px solid {C.BRAND};
  margin-bottom:8px;padding:11px 14px;background:rgba(23,86,74,.05)">
  <div style="display:flex;align-items:center;gap:10px">
    <span style="font-size:12.5px;color:#4A524E;flex:1;font-weight:600">TOTAL DEL PADRÓN</span>
    <span style="text-align:right;white-space:nowrap">
      <b style="font-family:Archivo;font-size:21px;letter-spacing:-.02em">{suma}</b>
      <span style="font-size:11px;color:#7C847F"> personas · {len(D)} delitos</span></span></div></div>''')
        H("".join(filas))
        if suma != len(P):
            st.warning(f"Las categorías suman {suma} y el padrón filtrado tiene {len(P)} "
                       "personas. Revisa los veredictos.")
    with c2:
        H('<div class="sectitle"><h2>Delitos por categoría</h2>'
          f'<p>{len(D)} registros en rojo</p></div>')
        with st.container(border=True):
            ch = charts.categorias(D)
            if ch is not None:
                st.altair_chart(ch, **ANCHO)
            else:
                st.info("Sin delitos en rojo en esta selección.")

    st.write("")
    H('<div class="sectitle"><h2>Delitos en rojo</h2>'
      '<p>Cada fila es un registro extraído del PDF, teñido por gravedad.</p></div>')
    if len(D):
        vis = D.sort_values(['NIVEL', 'NOMBRE', 'ORDEN_REPORTE'])[
            ['DNI', 'NOMBRE', 'LABOR', 'TIPO_PERSONAL', 'DELITO', 'NIVEL', 'CATEGORIA',
             'GRAVEDAD', 'ANIO', 'ACTIVO', 'VIGENTE', 'ESTADO', 'JURISDICCION', 'PARTE',
             'FUENTE', 'CASO', 'REGLA']]

        def tinte(row):
            color = C.COLOR_GRAVEDAD.get(row['GRAVEDAD'])
            return [ui.fila_tenida(color) if color else ''] * len(row)

        st.dataframe(vis.style.apply(tinte, axis=1), hide_index=True, **ANCHO, height=430)
        st.download_button("⬇️  Descargar estos delitos (CSV)",
                           vis.to_csv(index=False).encode('utf-8-sig'),
                           "delitos_en_rojo.csv", "text/csv")
    else:
        st.info("Sin delitos en rojo en esta selección.")

# =============================================================== VIGENCIA
elif seccion == "Vigencia":
    H('<div class="sectitle"><h2>Delitos por año del proceso</h2>'
      '<p>Un antecedente de 2014 archivado no pesa igual que una denuncia de 2025 en trámite.</p></div>')
    with st.container(border=True):
        ch = charts.por_anio(D)
        if ch is not None:
            st.altair_chart(ch, **ANCHO)
        else:
            st.info("Sin delitos fechados en esta selección.")

    st.write("")
    H('<div class="sectitle"><h2>Índice de riesgo contra antigüedad</h2>'
      '<p>Arriba a la derecha (banda roja) = grave, reciente y con proceso abierto. '
      'El tamaño del punto es el número de delitos.</p></div>')
    with st.container(border=True):
        ch = charts.dispersion_indice(P)
        if ch is not None:
            st.altair_chart(ch, **ANCHO)
        else:
            st.info("Sin delitos en esta selección.")

# ================================================================= PADRÓN
elif seccion == "Padrón":
    c1, c2, c3 = st.columns([2.4, 1.25, 1.05], gap="small")
    with c1:
        H(f'<div class="sectitle"><h2>Padrón · {len(P)} colaboradores</h2>'
          '<p>Clic en una fila para ver el detalle de sus delitos en rojo.</p></div>')
    with c2:
        orden = st.selectbox(
            "Ordenar por",
            ["Veredicto (crítico → no crítico)", "Nivel de la matriz (N1 → N6)",
             "Índice de riesgo", "Cantidad de delitos", "Registro más reciente",
             "Nombre", "DNI", "Labor", "Cuadrilla"],
            index=0, key="orden_padron",
            help="Criterio principal de ordenamiento del cuadro.")
    with c3, seg("sentido"):
        sentido = st.radio(
            "Sentido", ["Desc ↓", "Asc ↑"], index=0, horizontal=True, key="sentido_padron",
            help="Desc ↓ = lo más crítico primero. Asc ↑ = el orden exactamente invertido.")

    # columnas auxiliares para poder ordenar por criticidad (y no alfabéticamente)
    vis = P.copy()
    vis['ORDEN_VEREDICTO'] = vis['VEREDICTO'].map(C.ORDEN_VEREDICTO).fillna(99).astype(int)
    vis['ORDEN_NIVEL'] = vis['NIVEL_NUM'].fillna(99).astype(int)

    # (columnas, ascendente) en el sentido "Desc ↓" = lo más crítico arriba
    CRITERIOS = {
        "Veredicto (crítico → no crítico)": (['ORDEN_VEREDICTO', 'ORDEN_NIVEL', 'INDICE',
                                              C.COL_NOMBRE], [True, True, False, True]),
        "Nivel de la matriz (N1 → N6)":     (['ORDEN_NIVEL', 'ORDEN_VEREDICTO', 'INDICE',
                                              C.COL_NOMBRE], [True, True, False, True]),
        "Índice de riesgo":                 (['INDICE', 'ORDEN_VEREDICTO', C.COL_NOMBRE],
                                             [False, True, True]),
        "Cantidad de delitos":              (['N_DELITOS', 'INDICE', C.COL_NOMBRE],
                                             [False, False, True]),
        "Registro más reciente":            (['ANIO_MAX', 'INDICE', C.COL_NOMBRE],
                                             [False, False, True]),
        "Nombre":                           ([C.COL_NOMBRE], [True]),
        "DNI":                              (['DNI'], [True]),
        "Labor":                            (['LABOR', 'ORDEN_VEREDICTO', 'INDICE'],
                                             [True, True, False]),
        "Cuadrilla":                        (['CARPETAS', 'ORDEN_VEREDICTO', 'INDICE'],
                                             [True, True, False]),
    }
    cols_ord, asc_ord = CRITERIOS[orden]
    if sentido.startswith("Asc"):
        asc_ord = [not a for a in asc_ord]
    vis = vis.sort_values(by=cols_ord, ascending=asc_ord, na_position='last', kind='mergesort')

    st.caption(f"Ordenado por **{orden}** · **{sentido}** — "
               + ("del más crítico al menos crítico." if sentido.startswith("Desc")
                  else "orden invertido: del menos crítico al más crítico."))

    H(ui.padron(vis, df_d))
    H('<div class="leyenda" style="margin-top:10px">'
      '<span style="color:#7C847F">Tinte de fila:</span>' +
      "".join(f'<span><i class="sw" style="background:{ui.VEREDICTO_ESTILO[v]["riel"]}"></i>'
              f'{ui.esc(v[0] + v[1:].lower())}</span>' for v in C.VEREDICTOS) + '</div>')

    cols_csv = ['DNI', C.COL_NOMBRE, 'LABOR', 'TIPO_PERSONAL', 'CARPETAS', 'PDF_ADJUNTO',
                'ORDEN_VEREDICTO', 'NIVEL_MATRIZ', 'ACCION_MATRIZ', 'N_DELITOS', 'ANIO_MAX',
                'CASOS_ACTIVOS', 'INDICE', 'NIVEL_RIESGO', 'VEREDICTO', 'FILTRO']
    st.download_button("⬇️  Descargar este padrón (CSV)",
                       vis[cols_csv].to_csv(index=False).encode('utf-8-sig'),
                       "padron_filtrado.csv", "text/csv")

    with st.expander("Tabla plana (clic en cualquier cabecera para ordenar asc/desc)"):
        st.caption("La columna **Crit.** es el orden de criticidad del veredicto "
                   "(1 = NO APTO … 5 = pendiente de reporte): ordénala para ver el mismo "
                   "criterio que el cuadro de arriba.")
        st.dataframe(vis[cols_csv], hide_index=True, **ANCHO, height=420,
                     column_config={
                         "ORDEN_VEREDICTO": st.column_config.NumberColumn(
                             "Crit.", help="1 = más crítico · 5 = menos crítico", format="%d"),
                         "INDICE": st.column_config.ProgressColumn(
                             "Índice", min_value=0, max_value=100, format="%d")})

# ========================================================= CRITERIO N1-N6
elif seccion == "Criterio N1-N6":
    H('<div class="sectitle"><h2>Cómo se asigna el nivel N1-N6</h2>'
      '<p>El texto del delito se normaliza y se evalúa contra las reglas <b>en orden</b>. '
      'Gana la primera que coincide.</p></div>')
    for base in range(0, 6, 3):
        cols = st.columns(3, gap="small")
        for col, n in zip(cols, range(base + 1, base + 4)):
            reglas_n = [cat for niv, cat, _ in C.RULES if niv == n]
            fg = '#221704' if n == 4 else '#fff'
            with col:
                H(f'''<div class="lvl" style="--c:{C.COLOR_NIVEL[n]};--fg:{fg};margin-bottom:10px">
  <div class="hd">{ui.nchip(n)}<span class="cnt">{len(reglas_n)} regla{"" if len(reglas_n) == 1 else "s"}
    · {int((P['NIVEL_NUM'] == n).sum())} personas</span></div>
  <div class="ti">{ui.esc(C.NIVELES[n][0])}</div>
  <div class="ca">{ui.esc(" · ".join(reglas_n)) or "—"}</div>
  <span class="ac">{ui.esc(C.NIVELES[n][1])}</span></div>''')

    st.write("")
    H('<div class="sectitle"><h2>Las reglas, en orden de evaluación</h2></div>')
    reglas = pd.DataFrame([{
        "Orden": i, "Nivel": f"N{niv}", "Categoría": cat,
        "Palabras clave (regex)": " · ".join(
            p.replace('\\b', '').replace('^', '').replace('$', '').replace('\\.', '.')
            for p in pats),
        "Acción": C.NIVELES[niv][1]} for i, (niv, cat, pats) in enumerate(C.RULES, 1)])

    def tinte_regla(row):
        n = int(row['Nivel'][1])
        return [ui.fila_tenida(C.COLOR_NIVEL[n], .15)] * len(row)

    st.dataframe(reglas.style.apply(tinte_regla, axis=1), hide_index=True, **ANCHO, height=430)

    H(f'''<div class="nota"><b>Del delito a la persona.</b>
A cada persona se le asigna el nivel <b>más crítico</b> (el número menor) de todos sus delitos en rojo.
<ul>
<li>El orden de las reglas importa: «LESIONES LEVES (AGRESIONES EN CONTRA DE LAS MUJERES…)» cae en
<b>N5 familia</b>, no en N6 lesiones, porque la regla de familia se evalúa antes.</li>
<li>Niveles <b>1-3 → RETIRO</b>. Niveles <b>4-6 → SE ESTUDIA SALIDA</b>.</li>
<li>Con reporte y sin delitos en rojo → sin nivel, veredicto <b>APTO</b>.
Sin reporte → <b>PENDIENTE DE REPORTE</b>.</li>
<li>Si ningún patrón coincide → <b>N{C.NIVEL_SIN_MATCH} · {C.CATEGORIA_SIN_MATCH}</b>,
marcado para revisión manual.</li>
<li>Los delitos que la matriz de Prize no nombra (drogas, explosivos, usurpación, receptación,
estafa, secuestro) se asignaron <b>por analogía</b> y quedan marcados en la columna
<code>REGLA</code> del Excel.</li>
</ul>
<b>Índice de riesgo 0-100</b> — gravedad ({C.IDX_GRAVEDAD['GRAVE']}/{C.IDX_GRAVEDAD['MEDIO']}/{C.IDX_GRAVEDAD['LEVE']})
+ vigencia (20 si ≥{C.ANIO_VIGENCIA}; 10 si 2018-2022; 2 si anterior)
+ reincidencia (0/7/13/20 según 1/2/3/4+ delitos)
+ caso activo ({C.IDX_CASO_ACTIVO}) + dispersión geográfica ({C.IDX_DISPERSION}).<br>
Todo esto vive en <code>filtro/config.py</code>: cambiar un criterio es editar ese archivo.</div>''')

# =========================================================== TRAZABILIDAD
elif seccion == "Trazabilidad":
    H('<div class="sectitle"><h2>Qué se leyó</h2></div>')
    a, b, c, d_ = st.columns(4)
    a.metric("Filas en los Excel", meta['filas_excel'])
    b.metric("DNI únicos", meta['dni_unicos'], f"{meta['dni_repetidos']} repetidos",
             delta_color="off")
    c.metric("Reportes PDF", meta['n_pdfs'])
    d_.metric("DNI sin reporte", meta['sin_pdf'],
              f"{meta['sin_pdf'] / max(meta['dni_unicos'], 1) * 100:.0f}% del padrón",
              delta_color="inverse")
    if meta['pdfs_huerfanos']:
        st.warning(f"{len(meta['pdfs_huerfanos'])} PDF sin DNI correspondiente en los Excel: "
                   + ", ".join(meta['pdfs_huerfanos'][:20]))
    else:
        st.success("Todos los PDF encontrados corresponden a un DNI de los Excel.")

    H(f'''<div class="nota" style="margin-top:12px">
<b>Reglas de lectura</b>
<ul>
<li><b>Carpeta → labor / tipo de personal:</b>
{" · ".join(f"<code>*{k}*</code> → {lab} / {tipo}" for k, lab, tipo in C.LABOR_MAP)}</li>
<li><b>Campo FILTRO:</b> solo el valor del campo <i>Delito</i> cuando el texto del PDF está
pintado en <b>rojo</b>, concatenado con « || » <b>en el orden del reporte</b>. Los reportes usan
dos rojos, <code>#FC2727</code> y <code>#FF0000</code>; detectar uno solo pierde registros reales.</li>
<li><b>Texto gris = no aplica:</b> son casos donde la persona figura como denunciante o agraviado.</li>
<li><b>Secciones leídas:</b> DETALLE 2 en adelante (carpetas fiscales del MP, RENADESPPLE, INPE
y detenciones policiales). DETALLE 1 son incidencias policiales sin tipificación penal: se
cuentan pero no aportan delitos.</li>
<li><b>Fechas:</b> algunos reportes traen la fecha como serial de Excel
(<code>41901</code> = 19/09/2014); se convierten.</li>
</ul></div>''')

    st.divider()
    H('<div class="sectitle"><h2>Validación contra un archivo de referencia</h2>'
      '<p>Comprueba que un cambio de reglas no rompió nada.</p></div>')
    ref = st.file_uploader("Archivo de referencia (.xlsx)", type=["xlsx"])
    if ref is not None:
        tmp = os.path.join(raiz, ".cache_filtro", "referencia.xlsx")
        os.makedirs(os.path.dirname(tmp), exist_ok=True)
        with open(tmp, "wb") as f:
            f.write(ref.getbuffer())
        try:
            rep = validar(df_p, tmp)
        except Exception as e:
            st.error(f"No se pudo leer el archivo: {e}")
        else:
            cols = st.columns(2)
            for col, clave, etiqueta in ((cols[0], 'filtro', 'Campo FILTRO'),
                                         (cols[1], 'resumen', 'Resumen_Persona')):
                if rep[clave]:
                    ok, tot = rep[clave]['ok'], rep[clave]['total']
                    col.metric(etiqueta, f"{ok}/{tot}",
                               "idénticos" if ok == tot else f"{tot - ok} discrepancias",
                               delta_color="normal" if ok == tot else "inverse")
            if rep['detalles']:
                st.dataframe(pd.DataFrame(rep['detalles']), hide_index=True, **ANCHO)
            else:
                st.success("Cero discrepancias: el motor reproduce el archivo de referencia al 100%.")

    st.divider()
    st.caption("Un antecedente no equivale a una condena. Los estados «denuncia pendiente», "
               "«en calificación» o «con acusación» son procesos en curso; la decisión final "
               "es del comité. Información de uso restringido.")
