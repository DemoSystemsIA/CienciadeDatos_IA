# -*- coding: utf-8 -*-
"""Clasificación N1..N6, gravedad, vigencia y el índice de riesgo 0-100."""
from __future__ import annotations
import re
import datetime as dt
import unicodedata

from .config import (RULES, CATEGORIA_SIN_MATCH, NIVEL_SIN_MATCH, GRAV_CAT,
                     GRAVEDAD_POR_DEFECTO, PESO_GRAV, ACTIVO_NEG, LABOR_MAP,
                     LABOR_SIN_MATCH, IDX_GRAVEDAD, IDX_VIGENCIA, IDX_VIGENCIA_OLD,
                     IDX_REINCIDENCIA, IDX_REINCIDENCIA_MAX, IDX_CASO_ACTIVO,
                     IDX_DISPERSION, ANIO_VIGENCIA)

_RULES_C = [(niv, cat, [re.compile(p) for p in pats]) for niv, cat, pats in RULES]


def norm(s: str) -> str:
    """Mayúsculas sin tildes — la forma contra la que se evalúan las reglas."""
    s = unicodedata.normalize('NFD', (s or "").upper())
    return "".join(c for c in s if unicodedata.category(c) != 'Mn')


def clasificar(delito: str):
    """delito -> (nivel, categoría, patrón que coincidió)."""
    d = norm(delito)
    for niv, cat, pats in _RULES_C:
        for p in pats:
            if p.search(d):
                return niv, cat, p.pattern
    return NIVEL_SIN_MATCH, CATEGORIA_SIN_MATCH, ""


def gravedad(categoria: str) -> str:
    return GRAV_CAT.get(categoria, GRAVEDAD_POR_DEFECTO)


def caso_activo(estado: str) -> bool:
    """Un caso está activo si tiene estado y ese estado no indica cierre."""
    e = norm(estado)
    if not e or e.startswith('REGISTRO POLICIAL') or e == 'SIN ESTADO':
        return False
    return not any(k in e for k in ACTIVO_NEG)


def _serial_excel(v: str):
    """Algunos reportes traen la fecha como serial de Excel (41901 -> 2014)."""
    v = (v or "").strip()
    if re.fullmatch(r'\d{5}', v) and 20000 <= int(v) <= 60000:
        return (dt.date(1899, 12, 30) + dt.timedelta(days=int(v))).year
    return None


def anio(rec: dict):
    for k in ('Inicio del proceso', 'Fecha', 'Fecha Detención', 'Fecha Ingreso'):
        v = rec.get(k) or ""
        s = _serial_excel(v)
        if s:
            return s
        m = re.search(r'(19|20)\d{2}', v)
        if m:
            return int(m.group(0))
    return None


def labor_de(carpeta: str):
    c = norm(carpeta)
    for k, lab, tipo in LABOR_MAP:
        if k in c:
            return lab, tipo
    return LABOR_SIN_MATCH


def labor_personas(carpetas):
    pares = [labor_de(c) for c in (carpetas or [])]
    labs = sorted({a for a, _ in pares})
    tips = sorted({b for _, b in pares})
    return (" / ".join(labs) or LABOR_SIN_MATCH[0]), (" / ".join(tips) or LABOR_SIN_MATCH[1])


# --------------------------------------------------------------- índice
def indice_riesgo(delitos):
    """
    delitos: lista de dicts con GRAVEDAD, ANIO, ACTIVO, JURISDICCION.
    Devuelve (indice, (idx_gravedad, idx_vigencia, idx_reincidencia,
              idx_caso_activo, idx_dispersion), gravedad_max, anio_max)
    """
    if not delitos:
        return 0, (0, 0, 0, 0, 0), "", None

    grav = max((d['GRAVEDAD'] for d in delitos), key=lambda g: PESO_GRAV[g])
    i_grav = IDX_GRAVEDAD[grav]

    anios = [d['ANIO'] for d in delitos if d['ANIO']]
    amax = max(anios) if anios else None
    i_vig = 0
    if amax:
        i_vig = IDX_VIGENCIA_OLD
        for corte, pts in IDX_VIGENCIA:
            if amax >= corte:
                i_vig = pts
                break

    n = len(delitos)
    i_rei = IDX_REINCIDENCIA.get(n, IDX_REINCIDENCIA_MAX)
    i_act = IDX_CASO_ACTIVO if any(d['ACTIVO'] for d in delitos) else 0
    juris = {d['JURISDICCION'] for d in delitos if d['JURISDICCION']}
    i_dis = IDX_DISPERSION if len(juris) > 1 else 0

    total = min(100, i_grav + i_vig + i_rei + i_act + i_dis)
    return total, (i_grav, i_vig, i_rei, i_act, i_dis), grav, amax


def nivel_riesgo(gravedad_max: str, anio_max, tiene_pdf: bool, n_delitos: int) -> str:
    if n_delitos == 0:
        return "SIN REGISTRO" if tiene_pdf else "NO VERIFICADO"
    vigente = bool(anio_max and anio_max >= ANIO_VIGENCIA)
    if gravedad_max == "GRAVE":
        return "CRITICO"
    if gravedad_max == "MEDIO":
        return "ALTO" if vigente else "MEDIO"
    return "MEDIO" if vigente else "BAJO"


def veredicto(nivel_de_riesgo: str, tiene_pdf: bool, n_delitos: int) -> str:
    if n_delitos == 0:
        return "APTO" if tiene_pdf else "PENDIENTE DE REPORTE"
    return {"CRITICO": "NO APTO", "ALTO": "REVISION EN COMITE"}.get(
        nivel_de_riesgo, "APTO CON OBSERVACION")
