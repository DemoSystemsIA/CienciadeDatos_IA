# ==========================================================
# INFORME SSOMA - DISEÑO CORPORATIVO PROFESIONAL
# ==========================================================

import streamlit as st
import pandas as pd
import os, requests, tempfile
from io import BytesIO
from datetime import datetime

from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image,
    Table, TableStyle, HRFlowable, PageBreak,
    KeepTogether, Flowable
)
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT

# ══════════════════════════════════════════════════════════
# PALETA CORPORATIVA
# ══════════════════════════════════════════════════════════
C_NAVY      = colors.HexColor("#0D2340")
C_BLUE      = colors.HexColor("#0D2340")
C_BLUE_MID  = colors.HexColor("#0D2340")
C_BLUE_LITE = colors.HexColor("#E8EDF4")
C_ORANGE    = colors.HexColor("#0D2340")
C_ORANGE_LT = colors.HexColor("#E8EDF4")
C_GREEN     = colors.HexColor("#16A34A")
C_GREEN_LT  = colors.HexColor("#F0FDF4")
C_GRAY_D    = colors.HexColor("#334155")
C_GRAY_M    = colors.HexColor("#64748B")
C_GRAY_L    = colors.HexColor("#CBD5E1")
C_GRAY_BG   = colors.HexColor("#F8FAFC")
C_WHITE     = colors.white

# ══════════════════════════════════════════════════════════
# LAYOUT A4
# ══════════════════════════════════════════════════════════
PW, PH = A4
ML = 2.0 * cm
MR = 2.0 * cm
MT = 2.8 * cm
MB = 2.2 * cm
CW = PW - ML - MR

# ══════════════════════════════════════════════════════════
# STREAMLIT CONFIG
# ══════════════════════════════════════════════════════════
st.set_page_config(layout="wide", page_title="Informe SSOMA", page_icon="📋")

# ══════════════════════════════════════════════════════════
# URLS REMOTAS — Excel en SharePoint, Logo en GitHub
# ══════════════════════════════════════════════════════════
EXCEL_URL = (
    "https://aquanqape-my.sharepoint.com/:x:/g/personal/soporteti_aquanqa_pe/"
    "IQAKudFYiQ-bTIzlTJeXm5HbAVAc35k94KZWvES9s6BvOWc?e=H2hy1k&download=1"
)
LOGO_URL  = "https://github.com/CCozd/BI_SIG_CAMPO/blob/main/logo.png?raw=true"
SHEET     = "Informe"

# Ruta local temporal para el logo (se descarga una sola vez por sesión)
LOGO_TMP_PATH = os.path.join(tempfile.gettempdir(), "ssoma_logo.png")


# ══════════════════════════════════════════════════════════
# DESCARGAR LOGO AL ARRANCAR (si aún no está en disco)
# ══════════════════════════════════════════════════════════
def get_logo_path() -> str | None:
    """Descarga el logo desde GitHub y lo guarda en /tmp. Devuelve la ruta local."""
    if os.path.exists(LOGO_TMP_PATH):
        return LOGO_TMP_PATH
    try:
        r = requests.get(LOGO_URL, timeout=15)
        r.raise_for_status()
        with open(LOGO_TMP_PATH, "wb") as f:
            f.write(r.content)
        return LOGO_TMP_PATH
    except Exception as e:
        st.warning(f"No se pudo descargar el logo: {e}")
        return None


LOGO_PATH = get_logo_path()


# ══════════════════════════════════════════════════════════
# FLOWABLE: BANDA DE SECCIÓN
# ══════════════════════════════════════════════════════════
class SeccionBanda(Flowable):
    def __init__(self, numero, titulo, ancho=None):
        super().__init__()
        self.numero = numero
        self.titulo = titulo
        self.width  = ancho or CW
        self.height = 0.72 * cm

    def draw(self):
        c = self.canv
        w, h = self.width, self.height

        cx = 0.23*cm
        cy = h / 2
        c.setFillColor(C_BLUE)
        c.circle(cx, cy, 0.23*cm, fill=1, stroke=0)
        c.setFillColor(C_WHITE)
        c.setFont("Helvetica-Bold", 7.5)
        c.drawCentredString(cx, cy - 0.07*cm, self.numero)

        c.setStrokeColor(C_BLUE)
        c.setLineWidth(0.6)
        c.line(0, 0, w, 0)

        c.setFillColor(C_BLUE)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(0.23*cm + 0.42*cm, h/2 - 0.14*cm, self.titulo)


# ══════════════════════════════════════════════════════════
# HEADER / FOOTER
# ══════════════════════════════════════════════════════════
def make_header_footer(canv, doc):
    canv.saveState()

    if LOGO_PATH and os.path.exists(LOGO_PATH):
        try:
            from PIL import Image as PILImage
            pil     = PILImage.open(LOGO_PATH)
            iw, ih  = pil.size
            dw      = 3.8 * cm
            dh      = dw * (ih / iw)
            y_logo  = PH - 0.1*cm - dh
            canv.drawImage(
                LOGO_PATH, ML, y_logo,
                width=dw, height=dh,
                preserveAspectRatio=True, mask="auto"
            )
        except Exception:
            pass

    canv.setFillColor(C_GRAY_BG)
    canv.rect(0, 0, PW, MB - 0.05*cm, fill=1, stroke=0)

    canv.setStrokeColor(C_ORANGE)
    canv.setLineWidth(1.2)
    canv.line(0, MB - 0.05*cm, PW, MB - 0.05*cm)

    canv.setStrokeColor(C_NAVY)
    canv.setLineWidth(0.4)
    canv.line(0, MB - 0.18*cm, PW, MB - 0.18*cm)

    canv.setFillColor(C_GRAY_M)
    canv.setFont("Helvetica", 6.5)
    canv.drawString(ML, MB - 0.58*cm,
                    "Documento de uso interno — Prohibida su reproducción sin autorización")

    canv.setFillColor(C_NAVY)
    canv.setFont("Helvetica-Bold", 7.5)
    canv.drawCentredString(PW / 2, MB - 0.58*cm, f"— {doc.page} —")

    canv.setFillColor(C_GRAY_M)
    canv.setFont("Helvetica", 6.5)
    hoy = datetime.now().strftime("%d/%m/%Y %H:%M")
    canv.drawRightString(PW - MR, MB - 0.58*cm, f"Generado: {hoy}")

    canv.restoreState()


# ══════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════
def fmt_fecha(v):
    try:
        if isinstance(v, (pd.Timestamp, datetime)):
            return v.strftime("%d/%m/%Y")
        return pd.to_datetime(str(v)).strftime("%d/%m/%Y")
    except Exception:
        return str(v)

def safe(v):
    return "" if pd.isna(v) else str(v).strip()

def descargar_imagen(url: str):
    try:
        r = requests.get(url.strip(), timeout=15)
        r.raise_for_status()
        ct  = r.headers.get("Content-Type", "")
        ext = (".png" if "png" in ct else
               ".jpg" if "jpeg" in ct or "jpg" in ct else
               ".gif" if "gif"  in ct else
               os.path.splitext(url.split("?")[0])[-1] or ".jpg")
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
        tmp.write(r.content)
        tmp.close()
        return tmp.name
    except Exception as e:
        st.warning(f"No se pudo descargar: {url} — {e}")
        return None

def es_url(v):
    if pd.isna(v): return False
    s = str(v).strip()
    return s.startswith("http://") or s.startswith("https://")


# ══════════════════════════════════════════════════════════
# ESTILOS
# ══════════════════════════════════════════════════════════
def PS(name, **kw):
    return ParagraphStyle(name, **kw)

S_TITULO    = PS("titulo",  fontSize=13, fontName="Helvetica-Bold",
                 textColor=C_NAVY, alignment=TA_CENTER, spaceAfter=3, leading=17)
S_SUBTITULO = PS("subtit",  fontSize=8,  textColor=C_GRAY_M,
                 alignment=TA_CENTER, spaceAfter=8, leading=11)
S_TEXTO     = PS("texto",   fontSize=9.5, leading=15,
                 alignment=TA_JUSTIFY, textColor=C_GRAY_D, spaceAfter=2)
S_CELL_HDR  = PS("chdr",    fontSize=9,  fontName="Helvetica-Bold",
                 textColor=C_WHITE, alignment=TA_CENTER, leading=12)
S_CELL_VAL  = PS("cval",    fontSize=9,  alignment=TA_CENTER,
                 leading=13, textColor=C_GRAY_D)
S_PIE_FOTO  = PS("pie",     fontSize=8,  fontName="Helvetica-Oblique",
                 textColor=C_GRAY_M, alignment=TA_CENTER, spaceAfter=6, leading=11)
S_NUM_REC   = PS("numrec",  fontSize=9,  fontName="Helvetica-Bold",
                 textColor=C_WHITE, alignment=TA_CENTER, leading=12)
S_REC       = PS("rec",     fontSize=9.5, leading=14, textColor=C_GRAY_D)
S_OK        = PS("ok",      fontSize=9,  fontName="Helvetica-Bold",
                 textColor=C_GREEN, leading=13)


# ══════════════════════════════════════════════════════════
# GENERAR PDF
# ══════════════════════════════════════════════════════════
def generar_pdf(r):
    buf       = BytesIO()
    tmp_files = []

    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=ML, rightMargin=MR,
        topMargin=MT,  bottomMargin=MB
    )
    el = []

    # ── BLOQUE TÍTULO ────────────────────────────────────
    el.append(Spacer(1, 0.15*cm))

    t_titulo = Table([
        [Paragraph("INFORME DE CAPACITACIONES EN<br/>SEGURIDAD Y SALUD EN EL TRABAJO",
                   S_TITULO)],
        [Paragraph("Sistema de Gestión de Seguridad y Salud Ocupacional", S_SUBTITULO)],
    ], colWidths=[CW])
    t_titulo.setStyle(TableStyle([
        ("LINEABOVE",     (0,0), (-1, 0), 2,   C_BLUE),
        ("LINEBELOW",     (0,-1),(-1,-1), 0.6, C_BLUE),
        ("LEFTPADDING",   (0,0), (-1,-1), 0),
        ("RIGHTPADDING",  (0,0), (-1,-1), 0),
        ("TOPPADDING",    (0,0), (-1, 0), 10),
        ("BOTTOMPADDING", (0,-1),(-1,-1), 8),
    ]))
    el.append(t_titulo)
    el.append(Spacer(1, 0.4*cm))

    # ── DATOS CABECERA ───────────────────────────────────
    lbl_s  = PS("lbl_c",  fontSize=9,  fontName="Helvetica-Bold",
                textColor=C_NAVY, leading=14)
    sep_s  = PS("sep_c",  fontSize=9,  fontName="Helvetica-Bold",
                textColor=C_NAVY, leading=14, alignment=TA_CENTER)
    val_s  = PS("val_c",  fontSize=9,  leading=14, textColor=C_GRAY_D)
    val_it = PS("valit",  fontSize=9,  fontName="Helvetica-Oblique",
                leading=14, textColor=C_GRAY_M)

    def fila_cab(lbl, val, italic=False):
        return [
            Paragraph(lbl, lbl_s),
            Paragraph(":" if lbl else "", sep_s),
            Paragraph(safe(val), val_it if italic else val_s),
        ]

    cab_data = [
        fila_cab("PARA",   r["PARA"]),
        fila_cab("",       r["PUESTO_PARA"]),
        fila_cab("DE",     r["DE"]),
        fila_cab("",       r["PUESTO_DE"]),
        fila_cab("ASUNTO", r["ASUNTO"]),
        fila_cab("FECHA",  fmt_fecha(r["FECHA_INFORME"])),
    ]

    t_cab = Table(cab_data, colWidths=[2.6*cm, 0.5*cm, CW - 3.1*cm])
    t_cab.setStyle(TableStyle([
        ("VALIGN",        (0,0), (-1,-1), "TOP"),
        ("TOPPADDING",    (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ("LEFTPADDING",   (0,0), (0,-1),  0),
        ("LEFTPADDING",   (2,0), (2,-1),  4),
        ("LINEBELOW",     (0,5), (-1,5),  0.6, C_GRAY_L),
    ]))
    el.append(t_cab)
    el.append(Spacer(1, 0.5*cm))

    # ── I. OBJETIVO ──────────────────────────────────────
    el.append(SeccionBanda("I", "OBJETIVO"))
    el.append(Spacer(1, 0.2*cm))
    el.append(Paragraph(safe(r["OBJETIVO"]), S_TEXTO))
    el.append(Spacer(1, 0.4*cm))

    # ── II. ALCANCE ──────────────────────────────────────
    el.append(SeccionBanda("II", "ALCANCE"))
    el.append(Spacer(1, 0.2*cm))
    el.append(Paragraph(safe(r["ALCANCE"]), S_TEXTO))
    el.append(Spacer(1, 0.4*cm))

    # ── III. DESARROLLO ──────────────────────────────────
    el.append(SeccionBanda("III", "DESARROLLO DE LA ACTIVIDAD"))
    el.append(Spacer(1, 0.25*cm))

    intro_s = PS("intro", fontSize=9, fontName="Helvetica-Oblique",
                 textColor=C_GRAY_M, leading=13, spaceAfter=8)
    el.append(Paragraph("Se programaron las siguientes capacitaciones:", intro_s))

    col_w = [CW * 0.42, CW * 0.24, CW * 0.34]
    t_act = Table([
        [Paragraph("TEMA",    S_CELL_HDR),
         Paragraph("FECHA",   S_CELL_HDR),
         Paragraph("PONENTE", S_CELL_HDR)],
        [Paragraph(safe(r["TEMA_ACT"]),         S_CELL_VAL),
         Paragraph(fmt_fecha(r["FECHA_ACT"]),   S_CELL_VAL),
         Paragraph(safe(r["PONENTE_ACT"]),      S_CELL_VAL)],
    ], colWidths=col_w)

    t_act.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1, 0), C_NAVY),
        ("BACKGROUND",    (0,1), (-1, 1), C_BLUE_LITE),
        ("BOX",           (0,0), (-1,-1), 1.2, C_BLUE),
        ("INNERGRID",     (0,0), (-1,-1), 0.4, C_GRAY_L),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
        ("ALIGN",         (0,0), (-1,-1), "CENTER"),
        ("TOPPADDING",    (0,0), (-1,-1), 8),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
        ("LINEBELOW",     (0,0), (-1, 0), 2.5, C_ORANGE),
    ]))
    el.append(t_act)
    el.append(Spacer(1, 0.4*cm))

    # ── IV. INCIDENCIAS ──────────────────────────────────
    el.append(SeccionBanda("IV", "INCIDENCIAS"))
    el.append(Spacer(1, 0.2*cm))

    inc = safe(r.get("INCIDENCIAS", ""))
    if inc == "":
        el.append(Paragraph("Sin incidencias registradas durante esta actividad.", S_TEXTO))
    else:
        el.append(Paragraph(inc, S_TEXTO))
    el.append(Spacer(1, 0.4*cm))

    # ── V. RECOMENDACIONES ───────────────────────────────
    el.append(SeccionBanda("V", "RECOMENDACIONES"))
    el.append(Spacer(1, 0.2*cm))

    recs = [safe(r.get(f"RECOMENDACION{i}"))
            for i in range(1, 6)
            if pd.notna(r.get(f"RECOMENDACION{i}"))
            and safe(r.get(f"RECOMENDACION{i}")) != ""]

    if recs:
        bullet_s = PS("bul", fontSize=9.5, leading=15,
                      textColor=C_GRAY_D, leftIndent=12, spaceAfter=4)
        for idx, rec in enumerate(recs, start=1):
            el.append(Paragraph(f"{idx}.&nbsp;&nbsp;{rec}", bullet_s))
    el.append(Spacer(1, 0.4*cm))

    # ── VI. ANEXOS FOTOGRÁFICOS ───────────────────────────
    tiene_fotos = any(pd.notna(r.get(f"ANEXO{i}")) for i in range(1, 6))
    if tiene_fotos:
        el.append(PageBreak())
        el.append(SeccionBanda("VI", "ANEXOS FOTOGRÁFICOS"))
        el.append(Spacer(1, 0.35*cm))

        fig = 1
        for i in range(1, 6):
            foto = r.get(f"ANEXO{i}")
            desc = r.get(f"DESC{i}")
            if pd.isna(foto):
                continue

            foto_str = str(foto).strip()
            img_path = None
            if es_url(foto_str):
                img_path = descargar_imagen(foto_str)
                if img_path:
                    tmp_files.append(img_path)
            elif os.path.exists(foto_str):
                img_path = foto_str

            if img_path:
                try:
                    img_rl  = Image(img_path, width=CW - 0.6*cm, height=9.5*cm)
                    desc_tx = safe(desc) if pd.notna(desc) else f"Anexo {fig}"
                    pie_par = Paragraph(
                        f"Fig. {fig}&nbsp;&nbsp;—&nbsp;&nbsp;{desc_tx}", S_PIE_FOTO
                    )
                    t_foto = Table([[img_rl], [pie_par]], colWidths=[CW])
                    t_foto.setStyle(TableStyle([
                        ("BOX",           (0,0),(-1,-1), 1,   C_GRAY_L),
                        ("BACKGROUND",    (0,1),(-1,-1), C_GRAY_BG),
                        ("TOPPADDING",    (0,0),(0, 0),  4),
                        ("BOTTOMPADDING", (0,0),(0, 0),  0),
                        ("LEFTPADDING",   (0,0),(-1,-1), 4),
                        ("RIGHTPADDING",  (0,0),(-1,-1), 4),
                        ("TOPPADDING",    (0,1),(-1,-1), 5),
                        ("BOTTOMPADDING", (0,1),(-1,-1), 5),
                        ("LINEABOVE",     (0,1),(-1, 1), 1.2, C_ORANGE),
                    ]))
                    el.append(KeepTogether([t_foto, Spacer(1, 0.45*cm)]))
                    fig += 1
                except Exception as e:
                    el.append(Paragraph(f"[Error imagen {i}: {e}]", S_TEXTO))

    doc.build(el, onFirstPage=make_header_footer, onLaterPages=make_header_footer)
    buf.seek(0)

    for f in tmp_files:
        try: os.unlink(f)
        except: pass

    return buf


# ══════════════════════════════════════════════════════════
# CARGAR EXCEL DESDE SHAREPOINT
# ══════════════════════════════════════════════════════════
@st.cache_data(ttl=300)   # refresca cada 5 minutos
def cargar():
    """Descarga el Excel desde SharePoint y lo carga en un DataFrame."""
    try:
        response = requests.get(EXCEL_URL, timeout=30)
        response.raise_for_status()
        df = pd.read_excel(BytesIO(response.content), sheet_name=SHEET)
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        st.error(f"No se pudo cargar el Excel desde SharePoint: {e}")
        st.stop()

df = cargar()


# ══════════════════════════════════════════════════════════
# UI STREAMLIT
# ══════════════════════════════════════════════════════════
st.markdown("""
<style>
    .block-container           { padding-top: 1.8rem; }
    .stSelectbox label         { font-weight: 700; font-size: 0.8rem;
                                 color: #0D2340; text-transform: uppercase;
                                 letter-spacing: .04em; }
    .stButton > button         { border-radius: 8px; font-weight: 600;
                                 letter-spacing: .03em; height: 2.6rem; }
    .stDownloadButton > button { border-radius: 8px; font-weight: 700;
                                 letter-spacing: .03em; height: 2.6rem; }
</style>
""", unsafe_allow_html=True)

st.markdown("## 📋 Generador de Informe SSOMA")
st.caption("Selecciona los filtros, genera la vista previa y descarga el PDF corporativo.")
st.divider()

# ── Filtros ──────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
fechas_unicas  = df["FECHA_INFORME"].dropna().unique()
fecha_map      = {fmt_fecha(f): f for f in fechas_unicas}

fecha_label = c1.selectbox("📅 Fecha Informe",   list(fecha_map.keys()))
asunto      = c2.selectbox("📌 Asunto",          df["ASUNTO"].dropna().unique())
tema        = c3.selectbox("📚 Tema Actividad",  df["TEMA_ACT"].dropna().unique())
idcap       = c4.selectbox("🔢 ID Capacitación", df["ID_CAPACITACION"].dropna().unique())

fecha = fecha_map[fecha_label]
row   = df[
    (df["FECHA_INFORME"]   == fecha)  &
    (df["ASUNTO"]          == asunto) &
    (df["TEMA_ACT"]        == tema)   &
    (df["ID_CAPACITACION"] == idcap)
].iloc[0]

st.divider()

# ── Botones ──────────────────────────────────────────────
col_b1, col_b2 = st.columns(2)

with col_b1:
    if st.button("🔍 Generar Vista Previa", use_container_width=True, type="secondary"):
        with st.spinner("Generando informe..."):
            pdf_bytes = generar_pdf(row).read()
        st.session_state["pdf_bytes"] = pdf_bytes
        st.session_state["pdf_listo"] = True
        st.rerun()

with col_b2:
    if st.session_state.get("pdf_listo"):
        nombre = f"Informe_SSOMA_{fecha_label.replace('/','')}.pdf"
        st.download_button(
            label="⬇  Descargar PDF",
            data=st.session_state["pdf_bytes"],
            file_name=nombre,
            mime="application/pdf",
            use_container_width=True,
            type="primary"
        )
    else:
        st.button("⬇  Descargar PDF", disabled=True, use_container_width=True)

# ── Vista previa ─────────────────────────────────────────
if st.session_state.get("pdf_listo"):
    st.success("✅ Informe generado. Revisa la vista previa antes de descargar.")
    st.markdown("---")

    try:
        import fitz
        pdf_bytes = st.session_state["pdf_bytes"]

        # Abrir desde bytes en memoria — sin escribir nada en disco
        doc_prev = fitz.open(stream=pdf_bytes, filetype="pdf")
        n_pag    = len(doc_prev)

        st.markdown(f"### 🔍 Vista Previa — {n_pag} página{'s' if n_pag > 1 else ''}")

        for i, pagina in enumerate(doc_prev, start=1):
            _, col_c, _ = st.columns([0.5, 11, 0.5])
            with col_c:
                mat = fitz.Matrix(2.2, 2.2)
                pix = pagina.get_pixmap(matrix=mat, alpha=False, colorspace=fitz.csRGB)
                # Convertir a PNG en memoria (BytesIO) — evita el error de 'static/'
                img_buf = BytesIO(pix.tobytes("png"))
                st.image(img_buf, use_container_width=True,
                         caption=f"Página {i} de {n_pag}")
                if i < n_pag:
                    st.markdown("<div style='height:12px'></div>",
                                unsafe_allow_html=True)
        doc_prev.close()

    except Exception as e:
        st.error(f"No se pudo generar la vista previa: {e}")
