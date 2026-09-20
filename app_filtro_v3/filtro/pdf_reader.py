# -*- coding: utf-8 -*-
"""
Lectura de los reportes PDF de antecedentes.

La regla del comité: SOLO cuentan los registros pintados en ROJO — ahí la
persona figura como denunciada / imputada / sentenciada. El texto gris son
casos donde la persona es el denunciante o el agraviado, y NO aplica.

Los reportes usan dos rojos distintos (#FC2727 y #FF0000); si solo se detecta
uno se pierden registros reales, por eso el test es por rango y no por
igualdad exacta.
"""
from __future__ import annotations
import re
from collections import Counter, OrderedDict

import pdfplumber


# --------------------------------------------------------------- color
def is_red(color) -> bool:
    """True si el color de relleno del carácter es uno de los rojos del reporte."""
    if not color or len(color) != 3:
        return False
    r, g, b = color
    return r >= 0.6 and g <= 0.45 and b <= 0.45 and (r - max(g, b)) >= 0.3


# -------------------------------------------------------------- campos
LABELS = [
    "Distrito Fiscal", "Distrito", "Pais", "País", "Nacionalidad", "Tipo Documento",
    "Numero Documento", "Número Documento", "Documento", "Detenido", "Edad", "Delito",
    "Num. Caso", "Num. Orden", "Fecha Ingreso", "Fecha Egreso", "Motivo Libertad",
    "Entidad Informante", "Especialidad", "Dependencia Policial", "Dependencia",
    "Jurisdicción", "Jurisdiccion", "Fecha Detención", "Fecha Deteccion", "Fecha",
    "Caso", "Parte", "Inicio del proceso", "Estado", "Fiscalía", "Fiscalia",
    "Motivo Detención", "Juzgado", "Establecimiento", "Situación",
]
# etiquetas que siempre abren un bloque nuevo
START_LABELS = {"Distrito", "Distrito Fiscal", "Pais", "País"}

_MOJI = (("Ã‘", "Ñ"), ("Ã±", "ñ"), ("Ã\x81", "Á"), ("Ã©", "é"),
         ("Ã³", "ó"), ("Ãº", "ú"), ("Ã­", "í"), ("Ã\x93", "Ó"))


def limpiar(texto: str) -> str:
    """Arregla el mojibake del PDF, colapsa espacios y quita la elipsis de corte."""
    if not texto:
        return texto
    for a, b in _MOJI:
        texto = texto.replace(a, b)
    texto = re.sub(r'\s+', ' ', texto).strip().strip('"').strip()
    texto = re.sub(r'[…]+$', '', texto).strip()
    return texto


def pdf_lines(path):
    """
    Devuelve [(pagina, y, [(es_rojo, texto), ...]), ...]
    Agrupa caracteres por línea y parte cada línea en tramos por color.
    """
    out = []
    with pdfplumber.open(path) as pdf:
        for pi, pg in enumerate(pdf.pages):
            filas = {}
            for ch in pg.chars:
                key = round(ch['top'], 1)
                for k in filas:
                    if abs(k - key) < 3.0:
                        key = k
                        break
                filas.setdefault(key, []).append(ch)
            for k in sorted(filas):
                chs = sorted(filas[k], key=lambda c: c['x0'])
                runs, cur = [], None
                for ch in chs:
                    rojo = is_red(ch.get('non_stroking_color'))
                    if cur is None or cur[0] != rojo:
                        cur = [rojo, ""]
                        runs.append(cur)
                    cur[1] += ch['text']
                runs = [(a, b.strip()) for a, b in runs if b.strip()]
                if runs:
                    out.append((pi, k, runs))
    return out


def parse_pdf(path) -> dict:
    """
    Lee un reporte y devuelve:
      recs          : lista de registros en rojo (dict etiqueta -> valor)
      incid         : nº de incidencias policiales del DETALLE 1
      prov / provs  : provincia principal y todas las provincias del DETALLE 1
      gray_delitos  : delitos en gris (persona como denunciante) — no computan
    """
    lineas = pdf_lines(path)
    pares, zona, incid, provs, grises = [], 0, 0, [], []

    for _pi, _top, runs in lineas:
        junto = "".join(t for _, t in runs)

        m = re.match(r'^DETALLE\s*(\d+)', junto)
        if m:
            zona = int(m.group(1))
            continue

        if zona == 1:                      # incidencias policiales, sin tipificación penal
            if 'TIPIFICACIÓN' in junto:
                incid += 1
            mm = re.search(r'([A-ZÑÁÉÍÓÚ ]+)\s*/\s*([A-ZÑÁÉÍÓÚ ]+)\s*/\s*([A-ZÑÁÉÍÓÚ ]+)\s*/', junto)
            if mm:
                provs.append(mm.group(2).strip().title())
            continue
        if zona < 2:
            continue

        gris = [t for r, t in runs if not r]
        rojo = [t for r, t in runs if r]
        etiqueta = gris[0].strip() if gris else ""
        valor = limpiar(" ".join(rojo).strip())

        if etiqueta in LABELS:
            pares.append([etiqueta, valor])
        elif not gris and valor:           # continuación de un valor que se partió de línea
            if pares:
                pares[-1][1] = limpiar((pares[-1][1] + " " + valor).strip())
        elif gris and not rojo:            # fila íntegramente gris: no aplica
            g = gris[0]
            if g.startswith("Delito") and len(g) > 6:
                grises.append(limpiar(g[6:]))

    # ---- segmentar los pares en registros ----
    recs, cur = [], OrderedDict()

    def flush():
        if cur.get('Delito'):
            recs.append(dict(cur))

    for lab, val in pares:
        nuevo = lab in START_LABELS or (lab == "Delito" and "Delito" in cur)
        if nuevo and cur:
            flush()
            cur.clear()
        if lab in cur and not nuevo:
            if not cur[lab] and val:
                cur[lab] = val
        else:
            cur[lab] = val
    if cur:
        flush()

    return {
        'recs': recs,
        'incid': incid,
        'prov': Counter(provs).most_common(1)[0][0] if provs else "",
        'provs': sorted(set(provs)),
        'gray_delitos': grises,
        'pages': len({l[0] for l in lineas}),
    }
