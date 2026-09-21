# -*- coding: utf-8 -*-
"""Genera resultado_final.xlsx con el mismo formato del entregable del comité."""
from __future__ import annotations
import io
from collections import Counter

import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from . import config as C
from . import maestro as M

F = "Arial"
HDR_FILL = PatternFill("solid", fgColor="1F3864")
HDR_FONT = Font(F, bold=True, color="FFFFFF", size=10)
TITLE = Font(F, bold=True, size=14, color="1F3864")
THIN = Side(style="thin", color="BFBFBF")
BOX = Border(THIN, THIN, THIN, THIN)

FILL_RIESGO = {'CRITICO': "FFC7CE", 'ALTO': "FFD9A0", 'MEDIO': "FFF2CC", 'BAJO': "E2EFDA",
               'SIN REGISTRO': "E7E6E6", 'NO VERIFICADO': "F2F2F2"}
FILL_VER = {'NO APTO': "FF5B5B", 'REVISION EN COMITE': "FFC000",
            'APTO CON OBSERVACION': "FFE699", 'APTO': "A9D08E", 'PENDIENTE DE REPORTE': "D9D9D9"}
FILL_LABOR = {'ESTIBADOR': "DDEBF7", 'CHOFER KIA': "FCE4D6", 'PACKING': "E2EFDA",
              'SEGURIDAD PATRIMONIAL': "E7E0F4", 'CHOFER BUS': "D8EDEC",
              'CAMPO': "F1EBD2"}
FILL_TIPO = {'PROPIO': "E2EFDA", 'TERCERO': "FFF2CC"}
FILL_GRAV = {'GRAVE': "FFC7CE", 'MEDIO': "FFF2CC", 'LEVE': "E2EFDA"}
FILL_PRIZE = {'ACTIVO EN PLANILLA': "E2EFDA", 'CESADO': "FFF2CC",
              M.SIN_MATCH: "F8CBAD", 'SIN MAESTRO CARGADO': "F2F2F2"}


def _v(x):
    """Valor apto para openpyxl: convierte pd.NA / NaT / numpy scalars."""
    try:
        if x is None or pd.isna(x):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(x, (list, tuple, set)):
        return ", ".join(str(i) for i in x)
    if hasattr(x, 'item'):
        try:
            return x.item()
        except Exception:
            return str(x)
    return x


def _cabecera(ws, fila=1, ncol=None):
    ncol = ncol or ws.max_column
    for c in range(1, ncol + 1):
        cell = ws.cell(row=fila, column=c)
        cell.fill, cell.font = HDR_FILL, HDR_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    ws.freeze_panes = ws.cell(row=fila + 1, column=1)
    ws.row_dimensions[fila].height = 30


def _cuerpo(ws, inicio=2):
    for row in ws.iter_rows(min_row=inicio):
        for c in row:
            c.font = Font(F, size=10)
            c.border = BOX
            c.alignment = Alignment(vertical="top")


def _ancho(ws, maxw=55):
    for col in ws.columns:
        L = get_column_letter(col[0].column)
        w = max((len(str(c.value)) for c in col if c.value is not None), default=8)
        ws.column_dimensions[L].width = min(max(w + 2, 10), maxw)


def _pintar(ws, idx, columna, mapa, negrita=True):
    if columna not in idx:
        return
    col = idx[columna]
    for rw in range(2, ws.max_row + 1):
        c = ws.cell(rw, col)
        c.fill = PatternFill("solid", fgColor=mapa.get(c.value, "F2F2F2"))
        if negrita:
            c.font = Font(F, size=10, bold=True)


# ------------------------------------------------------------------ main
def construir_excel(df_p: pd.DataFrame, df_d: pd.DataFrame, meta: dict) -> bytes:
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    base_cols = [c for c in df_p.columns
                 if c not in ('CARPETAS_L', 'CATEGORIAS_L', 'JURIS_L', 'TIENE_PDF', 'NIVEL_NUM')
                 and not str(c).startswith('_')]
    orden_pref = ['DNI', C.COL_NOMBRE, 'LABOR', 'TIPO_PERSONAL', 'CARPETAS', 'PDF_ADJUNTO',
                  'EN_PRIZE', 'ESTADO_PRIZE', 'EMPRESA', 'COD_FUNCIONARIO', 'AREA', 'CARGO',
                  'CENTRO_COSTO', 'REGIMEN', 'TIPO_TRABAJADOR', 'PLANILLA',
                  'FECHA_INGRESO', 'FECHA_CESE', 'ANTIGUEDAD_ANIOS', 'N_CONTRATOS',
                  'FILTRO', 'N_DELITOS', 'NIVEL_MATRIZ', 'CONCEPTO_MATRIZ', 'ACCION_MATRIZ',
                  'NIVEL_RIESGO', 'GRAVEDAD_MAX', 'CATEGORIAS', 'ANIO_MAX', 'VIGENTE',
                  'CASOS_ACTIVOS', 'DELITOS_VIGENTES', 'JURISDICCIONES', 'N_JURISDICCIONES',
                  'PROVINCIA', 'INCID_POLICIALES', 'VEREDICTO', 'INDICE', 'IDX_GRAVEDAD',
                  'IDX_VIGENCIA', 'IDX_REINCIDENCIA', 'IDX_CASO_ACTIVO', 'IDX_DISPERSION']
    resto = [c for c in base_cols if c not in orden_pref]
    cols = list(dict.fromkeys([c for c in orden_pref if c in base_cols] + resto))

    # ---------------- Padrón (única hoja de personas) ----------------
    # Antes había dos hojas con las mismas filas (Hoja1 y Resumen_Persona);
    # ahora es una sola: mismas columnas, sin duplicar el padrón.
    ws = wb.create_sheet("Padron")
    ws.append([str(c) for c in cols])
    for _, r in df_p.iterrows():
        ws.append([_v(r.get(c)) for c in cols])
    _cabecera(ws); _cuerpo(ws)
    ci = {str(c): i + 1 for i, c in enumerate(cols)}
    _pintar(ws, ci, 'LABOR', FILL_LABOR)
    _pintar(ws, ci, 'TIPO_PERSONAL', FILL_TIPO)
    _pintar(ws, ci, 'NIVEL_RIESGO', FILL_RIESGO, negrita=False)
    _pintar(ws, ci, 'VEREDICTO', FILL_VER)
    _pintar(ws, ci, 'ESTADO_PRIZE', FILL_PRIZE, negrita=False)
    if 'INDICE' in ci:
        for rw in range(2, ws.max_row + 1):
            c = ws.cell(rw, ci['INDICE'])
            v = c.value or 0
            c.fill = PatternFill("solid", fgColor=("FF5B5B" if v >= 75 else "FFC000"
                                                   if v >= 50 else "FFE699" if v >= 30
                                                   else "E2EFDA"))
            c.font = Font(F, size=10, bold=True)
    if 'ACCION_MATRIZ' in ci:
        for rw in range(2, ws.max_row + 1):
            c = ws.cell(rw, ci['ACCION_MATRIZ'])
            if c.value == 'RETIRO':
                c.fill = PatternFill("solid", fgColor="FF0000")
                c.font = Font(F, size=10, bold=True, color="FFFFFF")
            elif c.value == 'SE ESTUDIA SALIDA':
                c.fill = PatternFill("solid", fgColor="FFFF00")
                c.font = Font(F, size=10, bold=True)
    _ancho(ws)
    if 'FILTRO' in ci:
        ws.column_dimensions[get_column_letter(ci['FILTRO'])].width = 70
    ws.auto_filter.ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"
    ws.freeze_panes = "C2"
    pi, n_per = ci, ws.max_row

    # ---------------- Delitos ----------------
    dcols = ['DNI', 'NOMBRE', 'LABOR', 'TIPO_PERSONAL', 'DELITO', 'NIVEL', 'CONCEPTO', 'ACCION',
             'CATEGORIA', 'GRAVEDAD', 'PESO_GRAVEDAD', 'ANIO', 'VIGENTE', 'ACTIVO',
             'JURISDICCION', 'FUENTE', 'ESTADO', 'PARTE', 'ESPECIALIDAD', 'ENTIDAD', 'CASO',
             'REGLA', 'ORDEN_REPORTE']
    dcols = [c for c in dcols if c in df_d.columns]
    ws = wb.create_sheet("Delitos")
    ws.append(dcols)
    dv = df_d.sort_values(['NIVEL', 'NOMBRE', 'ORDEN_REPORTE']) if len(df_d) else df_d
    for _, r in dv.iterrows():
        ws.append([("SI" if r[c] else "NO") if c in ('VIGENTE', 'ACTIVO') else _v(r.get(c))
                   for c in dcols])
    _cabecera(ws); _cuerpo(ws)
    di = {c: i + 1 for i, c in enumerate(dcols)}
    _pintar(ws, di, 'LABOR', FILL_LABOR)
    _pintar(ws, di, 'TIPO_PERSONAL', FILL_TIPO)
    _pintar(ws, di, 'GRAVEDAD', FILL_GRAV, negrita=False)
    for rw in range(2, ws.max_row + 1):
        c = ws.cell(rw, di['ACCION'])
        if c.value == 'RETIRO':
            c.fill = PatternFill("solid", fgColor="FF0000")
            c.font = Font(F, size=10, bold=True, color="FFFFFF")
        else:
            c.fill = PatternFill("solid", fgColor="FFFF00")
            c.font = Font(F, size=10, bold=True)
    _ancho(ws)
    ws.column_dimensions[get_column_letter(di['DELITO'])].width = 60
    if 'ENTIDAD' in di:
        ws.column_dimensions[get_column_letter(di['ENTIDAD'])].width = 45
    ws.auto_filter.ref = f"A1:{get_column_letter(ws.max_column)}{ws.max_row}"
    ws.freeze_panes = "C2"
    n_det = ws.max_row

    # ---------------- Resumen (estadísticas + matriz + planilla) ----------------
    # Antes la matriz vivía en su propia hoja repitiendo los mismos conteos que
    # las estadísticas; ahora es una sección más de esta hoja.
    ws = wb.create_sheet("Resumen")
    P = "Padron!"
    pr = lambda col: f"{P}${col}$2:${col}${n_per}"
    dr = lambda col: f"Delitos!${col}$2:${col}${n_det}"
    g = lambda k: get_column_letter(pi[k])
    gd = lambda k: get_column_letter(di[k])
    TOT = f"COUNTA({pr(g('DNI'))})"
    r = [1]

    def sec(t):
        ws.cell(r[0], 1, t).font = TITLE; r[0] += 2

    def head(*vs):
        for j, v in enumerate(vs, 1):
            c = ws.cell(r[0], j, v); c.fill = HDR_FILL; c.font = HDR_FONT
            c.alignment = Alignment(horizontal="center")
        r[0] += 1

    def line(lbl, f1, f2=None, fill=None):
        ws.cell(r[0], 1, lbl).font = Font(F, size=10); ws.cell(r[0], 1).border = BOX
        c = ws.cell(r[0], 2, f1); c.font = Font(F, size=10, bold=True); c.border = BOX
        if fill:
            c.fill = PatternFill("solid", fgColor=fill)
        if f2:
            c3 = ws.cell(r[0], 3, f2); c3.number_format = "0.0%"
            c3.font = Font(F, size=10); c3.border = BOX
        r[0] += 1

    sec("1. COBERTURA DEL FILTRO")
    head("INDICADOR", "CANT", "%")
    line("Personas evaluadas (DNI únicos)", f"={TOT}")
    line("Con reporte PDF adjunto", f'=COUNTIF({pr(g("PDF_ADJUNTO"))},"SI")', f'=IFERROR(B{r[0]}/{TOT},0)')
    line("SIN reporte PDF (pendiente)", f'=COUNTIF({pr(g("PDF_ADJUNTO"))},"NO")', f'=IFERROR(B{r[0]}/{TOT},0)')
    line("Con al menos un delito en rojo", f'=COUNTIF({pr(g("N_DELITOS"))},">0")', f'=IFERROR(B{r[0]}/{TOT},0)')
    line("Con reporte y SIN delitos en rojo",
         f'=COUNTIFS({pr(g("PDF_ADJUNTO"))},"SI",{pr(g("N_DELITOS"))},0)', f'=IFERROR(B{r[0]}/{TOT},0)')
    line("Total de delitos en rojo", f'=SUM({pr(g("N_DELITOS"))})')
    r[0] += 1

    sec("2. DECISIÓN SEGÚN MATRIZ DE CRITICIDAD")
    head("ACCION", "PERSONAS", "%")
    for a in ("RETIRO", "SE ESTUDIA SALIDA"):
        line(a, f'=COUNTIF({pr(g("ACCION_MATRIZ"))},"{a}")', f'=IFERROR(B{r[0]}/{TOT},0)')
    r[0] += 1
    head("NIVEL MATRIZ", "PERSONAS", "DELITOS")
    for n in range(1, 7):
        col = "FF0000" if n <= 3 else ("FFFF00" if n == 4 else "A9BE6A")
        c = ws.cell(r[0], 1, f"N{n} · {C.NIVELES[n][0]}")
        c.border = BOX; c.fill = PatternFill("solid", fgColor=col)
        c.font = Font(F, size=10, bold=True, color="FFFFFF" if n <= 3 else "000000")
        c2 = ws.cell(r[0], 2, f'=COUNTIF({pr(g("NIVEL_MATRIZ"))},"N{n}")')
        c2.font = Font(F, size=10, bold=True); c2.border = BOX
        c3 = ws.cell(r[0], 3, f'=COUNTIF({dr(gd("NIVEL"))},{n})')
        c3.font = Font(F, size=10); c3.border = BOX
        r[0] += 1
    r[0] += 1

    sec("3. NIVEL DE RIESGO Y VEREDICTO")
    head("NIVEL DE RIESGO", "PERSONAS", "%")
    for k in ("CRITICO", "ALTO", "MEDIO", "BAJO", "SIN REGISTRO", "NO VERIFICADO"):
        line(k, f'=COUNTIF({pr(g("NIVEL_RIESGO"))},"{k}")', f'=IFERROR(B{r[0]}/{TOT},0)',
             fill=FILL_RIESGO.get(k))
    r[0] += 1
    head("VEREDICTO", "PERSONAS", "%")
    for k in C.VEREDICTOS:
        line(k, f'=COUNTIF({pr(g("VEREDICTO"))},"{k}")', f'=IFERROR(B{r[0]}/{TOT},0)',
             fill=FILL_VER[k])
    r[0] += 1

    sec("4. DELITOS POR CATEGORÍA")
    head("CATEGORÍA", "DELITOS", "PERSONAS DISTINTAS")
    if len(df_d):
        for cat, n in df_d['CATEGORIA'].value_counts().items():
            ws.cell(r[0], 1, cat).font = Font(F, size=10); ws.cell(r[0], 1).border = BOX
            c = ws.cell(r[0], 2, f'=COUNTIF({dr(gd("CATEGORIA"))},"{cat}")')
            c.font = Font(F, size=10, bold=True); c.border = BOX
            c3 = ws.cell(r[0], 3, int(df_d[df_d['CATEGORIA'] == cat]['DNI'].nunique()))
            c3.font = Font(F, size=10); c3.border = BOX
            r[0] += 1
    r[0] += 1

    sec("5. SEÑALES DE ALERTA")
    head("INDICADOR", "CANT", "%")
    line("Delitos GRAVES en rojo", f'=COUNTIF({dr(gd("GRAVEDAD"))},"GRAVE")')
    line("Delitos vigentes 2023+", f'=COUNTIF({dr(gd("VIGENTE"))},"SI")')
    line("Delitos con caso ACTIVO", f'=COUNTIF({dr(gd("ACTIVO"))},"SI")')
    line("Personas con caso activo", f'=COUNTIF({pr(g("CASOS_ACTIVOS"))},">0")', f'=IFERROR(B{r[0]}/{TOT},0)')
    line("Personas reincidentes (2+)", f'=COUNTIF({pr(g("N_DELITOS"))},">=2")', f'=IFERROR(B{r[0]}/{TOT},0)')
    line("Personas multi-jurisdicción (2+)", f'=COUNTIF({pr(g("N_JURISDICCIONES"))},">=2")',
         f'=IFERROR(B{r[0]}/{TOT},0)')
    line("Índice promedio (con delitos)",
         f'=IFERROR(ROUND(AVERAGEIF({pr(g("N_DELITOS"))},">0",{pr(g("INDICE"))}),1),0)')
    line("Índice máximo", f'=MAX({pr(g("INDICE"))})')
    r[0] += 1

    sec("6. CORTE POR LABOR Y TIPO DE PERSONAL")
    head("LABOR", "PERSONAS", "% RETIRO")
    for lb in sorted(df_p['LABOR'].unique()):
        c = ws.cell(r[0], 1, lb); c.border = BOX
        c.font = Font(F, size=10, bold=True)
        c.fill = PatternFill("solid", fgColor=FILL_LABOR.get(lb, "F2F2F2"))
        c2 = ws.cell(r[0], 2, f'=COUNTIF({pr(g("LABOR"))},"{lb}")')
        c2.font = Font(F, size=10, bold=True); c2.border = BOX
        c3 = ws.cell(r[0], 3, f'=IFERROR(COUNTIFS({pr(g("LABOR"))},"{lb}",'
                              f'{pr(g("VEREDICTO"))},"NO APTO")/COUNTIF({pr(g("LABOR"))},"{lb}"),0)')
        c3.number_format = "0.0%"; c3.font = Font(F, size=10); c3.border = BOX
        r[0] += 1
    r[0] += 1
    head("TIPO DE PERSONAL", "PERSONAS", "% RETIRO")
    for tp in sorted(df_p['TIPO_PERSONAL'].unique()):
        c = ws.cell(r[0], 1, tp); c.border = BOX
        c.font = Font(F, size=10, bold=True)
        c.fill = PatternFill("solid", fgColor=FILL_TIPO.get(tp, "F2F2F2"))
        c2 = ws.cell(r[0], 2, f'=COUNTIF({pr(g("TIPO_PERSONAL"))},"{tp}")')
        c2.font = Font(F, size=10, bold=True); c2.border = BOX
        c3 = ws.cell(r[0], 3, f'=IFERROR(COUNTIFS({pr(g("TIPO_PERSONAL"))},"{tp}",'
                              f'{pr(g("VEREDICTO"))},"NO APTO")/COUNTIF({pr(g("TIPO_PERSONAL"))},"{tp}"),0)')
        c3.number_format = "0.0%"; c3.font = Font(F, size=10); c3.border = BOX
        r[0] += 1
    r[0] += 1
    head("LABOR × VEREDICTO", "PERSONAS", "%")
    for lb in sorted(df_p['LABOR'].unique()):
        for vv in C.VEREDICTOS:
            ws.cell(r[0], 1, f"{lb} · {vv}").font = Font(F, size=10)
            ws.cell(r[0], 1).border = BOX
            c2 = ws.cell(r[0], 2, f'=COUNTIFS({pr(g("LABOR"))},"{lb}",{pr(g("VEREDICTO"))},"{vv}")')
            c2.font = Font(F, size=10, bold=True); c2.border = BOX
            c2.fill = PatternFill("solid", fgColor=FILL_VER[vv])
            c3 = ws.cell(r[0], 3, f'=IFERROR(B{r[0]}/COUNTIF({pr(g("LABOR"))},"{lb}"),0)')
            c3.number_format = "0.0%"; c3.font = Font(F, size=10); c3.border = BOX
            r[0] += 1
    r[0] += 1

    sec("7. MATRIZ DE CRITICIDAD")
    head("NIVEL", "CONCEPTOS REFERENCIALES", "CRITICIDAD / ACCIÓN", "PERSONAS", "DELITOS")
    cnt_p = Counter(df_p['NIVEL_NUM'].dropna().astype(int)) if len(df_p) else Counter()
    cnt_d = Counter(df_d['NIVEL']) if len(df_d) else Counter()
    for n in range(1, 7):
        concepto, accion, crit = C.NIVELES[n]
        color = "FF0000" if n <= 3 else ("FFFF00" if n == 4 else "A9BE6A")
        vals = [f"N{n}", concepto, f"{crit} · {accion}", cnt_p.get(n, 0), cnt_d.get(n, 0)]
        for j, v in enumerate(vals, 1):
            c = ws.cell(r[0], j, v)
            c.font = Font(F, size=10, bold=(j in (1, 4)))
            c.border = BOX
            c.alignment = Alignment(vertical="center", wrap_text=(j == 2))
        for j in (1, 3):
            ws.cell(r[0], j).fill = PatternFill("solid", fgColor=color)
            ws.cell(r[0], j).font = Font(F, bold=True, size=10,
                                         color="FFFFFF" if n <= 3 else "000000")
        r[0] += 1
    for clave, e in C.EXTRA_MATRIZ.items():
        vals = [clave, e['concepto'], e['accion'],
                int((df_p['VEREDICTO'] == e['veredicto']).sum()), 0]
        for j, v in enumerate(vals, 1):
            c = ws.cell(r[0], j, v)
            c.font = Font(F, size=10, bold=(j in (1, 4))); c.border = BOX
            c.alignment = Alignment(vertical="center", wrap_text=(j == 2))
        for j in (1, 3):
            ws.cell(r[0], j).fill = PatternFill(
                "solid", fgColor=("A9D08E" if e['veredicto'] == 'APTO' else "D9D9D9"))
        r[0] += 1
    for t in ("PERSONAS se asigna por el nivel MÁS CRÍTICO (menor) de cada persona.",
              "DELITOS cuenta cada registro en rojo: una persona puede aportar varios.",
              "Las dos últimas filas son gente sin delitos en rojo; la columna PERSONAS "
              "de toda la tabla suma el padrón completo."):
        ws.cell(r[0], 1, "NOTA: " + t).font = Font(F, size=9, italic=True, color="808080")
        r[0] += 1
    r[0] += 1

    sec("8. PLANILLA PRIZE (CRUCE POR DNI)")
    im = meta.get('maestro') or {}
    if not im.get('hay_maestro'):
        ws.cell(r[0], 1, "No se cargó el maestro de funcionarios: sin cruce por DNI. "
                         "Coloca el export de qbiz en la carpeta raíz o súbelo desde el "
                         "tablero.").font = Font(F, size=10, italic=True, color="808080")
        r[0] += 2
    else:
        head("INDICADOR", "CANT", "%")
        line("Personas del padrón en planilla Prize",
             f'=COUNTIF({pr(g("EN_PRIZE"))},"SI")', f'=IFERROR(B{r[0]}/{TOT},0)')
        line(f"Personas marcadas «{M.SIN_MATCH}»",
             f'=COUNTIF({pr(g("EN_PRIZE"))},"NO")', f'=IFERROR(B{r[0]}/{TOT},0)',
             fill="F8CBAD")
        line("En planilla y ACTIVO",
             f'=COUNTIF({pr(g("ESTADO_PRIZE"))},"ACTIVO EN PLANILLA")',
             f'=IFERROR(B{r[0]}/{TOT},0)')
        line("En planilla y CESADO",
             f'=COUNTIF({pr(g("ESTADO_PRIZE"))},"CESADO")', f'=IFERROR(B{r[0]}/{TOT},0)')
        line("Filas del maestro leídas", im.get('filas_maestro', 0))
        r[0] += 1
        head("ÁREA (SEGÚN PLANILLA)", "PERSONAS", "% RETIRO")
        for ar in sorted(x for x in df_p['AREA'].dropna().unique() if str(x).strip()):
            c = ws.cell(r[0], 1, ar); c.border = BOX; c.font = Font(F, size=10, bold=True)
            if ar == M.SIN_MATCH:
                c.fill = PatternFill("solid", fgColor="F8CBAD")
            c2 = ws.cell(r[0], 2, f'=COUNTIF({pr(g("AREA"))},"{ar}")')
            c2.font = Font(F, size=10, bold=True); c2.border = BOX
            c3 = ws.cell(r[0], 3, f'=IFERROR(COUNTIFS({pr(g("AREA"))},"{ar}",'
                                  f'{pr(g("VEREDICTO"))},"NO APTO")/COUNTIF({pr(g("AREA"))},"{ar}"),0)')
            c3.number_format = "0.0%"; c3.font = Font(F, size=10); c3.border = BOX
            r[0] += 1
        r[0] += 1
        head("ESTADO EN PRIZE × VEREDICTO", "PERSONAS", "%")
        for ep in sorted(x for x in df_p['ESTADO_PRIZE'].dropna().unique() if str(x).strip()):
            for vv in C.VEREDICTOS:
                ws.cell(r[0], 1, f"{ep} · {vv}").font = Font(F, size=10)
                ws.cell(r[0], 1).border = BOX
                c2 = ws.cell(r[0], 2, f'=COUNTIFS({pr(g("ESTADO_PRIZE"))},"{ep}",'
                                      f'{pr(g("VEREDICTO"))},"{vv}")')
                c2.font = Font(F, size=10, bold=True); c2.border = BOX
                c2.fill = PatternFill("solid", fgColor=FILL_VER[vv])
                c3 = ws.cell(r[0], 3, f'=IFERROR(B{r[0]}/COUNTIF({pr(g("ESTADO_PRIZE"))},"{ep}"),0)')
                c3.number_format = "0.0%"; c3.font = Font(F, size=10); c3.border = BOX
                r[0] += 1

    _ancho(ws)
    ws.column_dimensions['A'].width = 62
    ws.column_dimensions['B'].width = 14
    ws.column_dimensions['C'].width = 22

    # ---------------- Criterio_N1_N6 ----------------
    ws = wb.create_sheet("Criterio_y_Metodo")
    ws.cell(1, 1, "CÓMO SE ASIGNA EL NIVEL N1-N6 A CADA DELITO").font = TITLE
    c = ws.cell(2, 1, "El texto del delito se normaliza (mayúsculas, sin tildes) y se evalúa contra "
                      "los patrones de abajo EN ESTE ORDEN. Gana el primero que coincide: por eso "
                      "'LESIONES LEVES (AGRESIONES EN CONTRA DE LAS MUJERES...)' cae en N5 familia "
                      "y no en N6 lesiones.")
    c.font = Font(F, size=10, italic=True, color="595959")
    c.alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells(start_row=2, start_column=1, end_row=3, end_column=5)
    for j, v in enumerate(["ORDEN", "NIVEL", "CATEGORÍA ASIGNADA",
                           "PALABRAS CLAVE QUE LA ACTIVAN (regex)", "ACCIÓN"], 1):
        ws.cell(5, j, v)
    _cabecera(ws, fila=5, ncol=5)
    rw = 6
    for i, (niv, cat, pats) in enumerate(C.RULES, 1):
        color = "FF0000" if niv <= 3 else ("FFFF00" if niv == 4 else "A9BE6A")
        vals = [i, f"N{niv}", cat,
                " · ".join(p.replace('\\b', '').replace('^', '').replace('$', '').replace('\\.', '.')
                           for p in pats),
                C.NIVELES[niv][1]]
        for j, v in enumerate(vals, 1):
            cc = ws.cell(rw, j, v); cc.font = Font(F, size=10); cc.border = BOX
            cc.alignment = Alignment(vertical="top", wrap_text=(j == 4))
        for j in (2, 5):
            ws.cell(rw, j).fill = PatternFill("solid", fgColor=color)
            ws.cell(rw, j).font = Font(F, size=10, bold=True,
                                       color="FFFFFF" if niv <= 3 else "000000")
        ws.cell(rw, 2).alignment = Alignment(horizontal="center")
        rw += 1
    for j, v in ((1, "Sin coincidencia"), (2, f"N{C.NIVEL_SIN_MATCH}"), (3, C.CATEGORIA_SIN_MATCH),
                 (4, "Ningún patrón coincide — queda genérico y se marca para revisión manual."),
                 (5, C.NIVELES[C.NIVEL_SIN_MATCH][1])):
        cc = ws.cell(rw, j, v); cc.font = Font(F, size=10, italic=True); cc.border = BOX
        cc.alignment = Alignment(wrap_text=True, vertical="top")
    ws.cell(rw, 2).fill = PatternFill("solid", fgColor="A9BE6A")
    rw += 2
    ws.cell(rw, 1, "DEL DELITO A LA PERSONA").font = Font(F, bold=True, size=12, color="1F3864")
    rw += 1
    for t in ["A cada persona se le asigna el nivel MÁS CRÍTICO (el menor) de sus delitos en rojo.",
              "Niveles 1-3 → RETIRO. Niveles 4-6 → SE ESTUDIA SALIDA.",
              "Con reporte y sin delitos en rojo → sin nivel, veredicto APTO.",
              "Sin reporte PDF → sin nivel, veredicto PENDIENTE DE REPORTE.",
              "Los delitos que la matriz no nombra (drogas, explosivos, usurpación, receptación, "
              "estafa, secuestro) se asignaron por analogía y quedan marcados en la columna REGLA "
              "de Detalle_Delitos para que el comité los reclasifique si corresponde."]:
        cc = ws.cell(rw, 1, "• " + t); cc.font = Font(F, size=10)
        cc.alignment = Alignment(wrap_text=True, vertical="top")
        ws.merge_cells(start_row=rw, start_column=1, end_row=rw, end_column=5)
        rw += 1
    for col, w in zip("ABCDE", (12, 9, 34, 78, 20)):
        ws.column_dimensions[col].width = w

    # ---------------- Trazabilidad, en la misma hoja del criterio ----------------
    rw += 2
    ws.cell(rw, 1, "TRAZABILIDAD DEL PROCESO").font = TITLE
    rw += 2
    im = meta.get('maestro') or {}
    filas = [
        ("Carpeta raíz", meta['raiz']),
        ("Cuadrillas detectadas", ", ".join(meta['carpetas'])),
        ("Labor / Tipo de personal",
         "Se deriva del nombre de la carpeta: " +
         " · ".join(f"*{k}* → {lab} / {tipo}" for k, lab, tipo in C.LABOR_MAP)),
        ("Archivos Excel leídos", str(meta['n_excels'])),
        ("Filas leídas", str(meta['filas_excel'])),
        ("DNI únicos", str(meta['dni_unicos'])),
        ("DNI repetidos entre carpetas", str(meta['dni_repetidos'])),
        ("PDFs en subcarpetas Adjuntos", str(meta['n_pdfs'])),
        ("DNI con PDF", str(meta['con_pdf'])),
        ("DNI sin PDF", str(meta['sin_pdf'])),
        ("PDFs sin DNI en los Excel", str(len(meta['pdfs_huerfanos']))),
        ("", ""),
        ("Regla del campo 'FILTRO'",
         "Solo el valor del campo 'Delito' cuando el texto del PDF está pintado en ROJO, "
         "concatenado con ' || ' EN EL ORDEN DEL REPORTE."),
        ("Rojos detectados", "#FC2727 (0.98824, 0.15294, 0.15294) y #FF0000 (1, 0, 0)"),
        ("Texto NO computado",
         "Gris #6C757D — registros donde la persona figura como denunciante/agraviado."),
        ("Secciones leídas",
         "DETALLE 2 en adelante (carpetas fiscales MP, RENADESPPLE, INPE y detenciones "
         "policiales). DETALLE 1 = incidencias policiales: se cuentan pero no aportan delitos."),
        ("", ""),
        ("Índice de riesgo (0-100)",
         "IDX GRAVEDAD (40/22/10 según GRAVE/MEDIO/LEVE) + IDX VIGENCIA (20 si ≥2023; 10 si "
         "2018-2022; 2 si anterior) + IDX REINCIDENCIA (0/7/13/20 según 1/2/3/4+ delitos) + "
         "IDX CASO ACTIVO (12 si algún caso no archivado) + IDX DISPERSIÓN (8 si 2+ "
         "jurisdicciones). Tope 100."),
        ("Nivel de riesgo",
         "GRAVE → CRITICO. MEDIO → ALTO si vigente 2023+, si no MEDIO. LEVE → MEDIO si vigente "
         "2023+, si no BAJO."),
        ("Veredicto",
         "CRITICO → NO APTO · ALTO → REVISION EN COMITE · MEDIO/BAJO → APTO CON OBSERVACION · "
         "con reporte y sin delitos rojos → APTO · sin reporte → PENDIENTE DE REPORTE."),
        ("Acción matriz", "Niveles 1-3 → RETIRO · Niveles 4-6 → SE ESTUDIA SALIDA."),
        ("", ""),
        ("", ""),
        ("Maestro de funcionarios",
         (f"{im.get('archivo')} — {im.get('filas_maestro', 0)} DNI únicos. "
          f"{im.get('con_match', 0)} personas del padrón cruzaron; "
          f"{im.get('sin_match', 0)} quedaron como «{M.SIN_MATCH}»."
          if im.get('hay_maestro') else
          "No cargado: las columnas de planilla quedan vacías.")),
        ("Cómo se cruza",
         "Por DNI, comparando solo dígitos y sin ceros a la izquierda. Si un DNI trae "
         "varios contratos se conserva el vigente y, entre ellos, el de modificación "
         "más reciente; N_CONTRATOS dice cuántos había."),
        ("", ""),
        ("Hojas de este archivo",
         "Padron (una fila por persona) · Delitos (una fila por registro en rojo) · "
         "Resumen (todos los conteos y la matriz) · Criterio_y_Metodo (esta hoja). "
         "No hay datos repetidos entre hojas."),
        ("", ""),
        ("Confidencialidad",
         "Información de uso restringido. No difundir ni comentar fuera del comité evaluador."),
    ]
    for a, b in filas:
        ca = ws.cell(rw, 1, a); cb = ws.cell(rw, 2, b)
        ca.font = Font(F, size=10, bold=True)
        cb.font = Font(F, size=10)
        cb.alignment = Alignment(wrap_text=True, vertical="top")
        rw += 1
    for col, w in zip("ABCDE", (42, 105, 34, 78, 20)):
        ws.column_dimensions[col].width = w

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
