# -*- coding: utf-8 -*-
"""
Gráficos Altair del tablero.

Misma paleta que el resto de la interfaz (config.COLOR*): rojo = retiro,
ámbar = N4, oliva = N5-N6, verde = sin observaciones, gris = sin verificar.
Todas las barras llevan su valor escrito: el color agrupa, el número decide.
"""
from __future__ import annotations
import altair as alt
import pandas as pd

from . import config as C

VERSION = "3.2"   # debe coincidir con filtro/config.py

FONT = '"IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif'
FONT_NUM = '"IBM Plex Mono", ui-monospace, monospace'

TEMAS = {
    "claro":  dict(GRID="#E4E3DB", AXIS="#7C847F", INK="#14171A", INK2="#4A524E"),
    "oscuro": dict(GRID="#2C3540", AXIS="#8E98A2", INK="#EDF1F4", INK2="#C3CBD3"),
}
GRID, AXIS = TEMAS["claro"]["GRID"], TEMAS["claro"]["AXIS"]
INK, INK2 = TEMAS["claro"]["INK"], TEMAS["claro"]["INK2"]


def aplicar_tema(nombre: str = "claro") -> None:
    """Ejes, rejilla y textos de los gráficos según el tema activo."""
    global GRID, AXIS, INK, INK2
    t = TEMAS.get(nombre, TEMAS["claro"])
    GRID, AXIS, INK, INK2 = t["GRID"], t["AXIS"], t["INK"], t["INK2"]


def _base(ch, h):
    return (ch.properties(height=h, background='transparent')
            .configure_view(strokeWidth=0)
            .configure_axis(labelFont=FONT, titleFont=FONT, labelColor=AXIS, titleColor=AXIS,
                            labelFontSize=11, titleFontSize=10.5, titleFontWeight='normal',
                            gridColor=GRID, domainColor=GRID, tickColor=GRID, tickSize=4)
            .configure_legend(labelFont=FONT, titleFont=FONT, labelFontSize=11.5,
                              titleFontSize=11, labelColor=INK2, titleColor=AXIS,
                              symbolType='square', symbolSize=110)
            .configure_title(font=FONT, fontSize=13, anchor='start', color=INK2))


# ------------------------------------------------- matriz de criticidad
def niveles_matriz(df_p: pd.DataFrame, df_d: pd.DataFrame):
    """
    N1..N6 más las dos categorías de gente sin delitos en rojo
    (SIN OBSERVACIONES y SIN VERIFICAR), para que la suma cierre con el padrón.
    """
    filas = []
    for n in range(1, 7):
        filas.append({
            'Nivel': f"N{n}",
            'Concepto': C.NIVELES[n][0],
            'Personas': int((df_p['NIVEL_NUM'] == n).sum()),
            'Delitos': int((df_d['NIVEL'] == n).sum()) if len(df_d) else 0,
            'Acción': C.NIVELES[n][1],
            'color': C.COLOR_NIVEL[n]})
    for clave, e in C.EXTRA_MATRIZ.items():
        filas.append({
            'Nivel': e['etiqueta'],
            'Concepto': clave.title() + " · " + e['concepto'].lower(),
            'Personas': int((df_p['VEREDICTO'] == e['veredicto']).sum()),
            'Delitos': 0,
            'Acción': e['accion'],
            'color': e['color']})
    d = pd.DataFrame(filas)
    orden = [f"N{n}" for n in range(1, 7)] + [e['etiqueta'] for e in C.EXTRA_MATRIZ.values()]
    y = alt.Y('Nivel:N', sort=orden, title=None,
              axis=alt.Axis(labelOverlap=False, labelFontSize=13, labelFontWeight='bold',
                            labelColor=INK, labelPadding=6, domain=False, ticks=False))
    tip = ['Nivel', 'Concepto', 'Personas', 'Delitos', 'Acción']
    fondo = (alt.Chart(d).mark_bar(cornerRadius=5, size=22, opacity=.13)
             .encode(x=alt.X('max(Personas):Q', title=None, axis=None),
                     y=y, color=alt.Color('color:N', scale=None, legend=None)))
    barra = (alt.Chart(d).mark_bar(cornerRadius=5, size=22)
             .encode(x=alt.X('Personas:Q', title='personas',
                             axis=alt.Axis(grid=True, tickCount=5)),
                     y=y, color=alt.Color('color:N', scale=None, legend=None), tooltip=tip))
    txt = (alt.Chart(d).mark_text(align='left', dx=7, fontSize=13, font=FONT,
                                  fontWeight='bold', color=INK)  # noqa: usa el tema activo
           .encode(x='Personas:Q', y=y, text='Personas:Q'))
    return _base((fondo + barra + txt), 30 * len(d) + 55)


# ----------------------------------------------------- delitos por categoría
def categorias(df_d: pd.DataFrame, top=14):
    if not len(df_d):
        return None
    g = (df_d.groupby(['CATEGORIA', 'NIVEL']).size().reset_index(name='Delitos')
         .groupby('CATEGORIA').agg(Delitos=('Delitos', 'sum'), NIVEL=('NIVEL', 'min'))
         .reset_index().sort_values('Delitos', ascending=False).head(top))
    g['color'] = g['NIVEL'].map(C.COLOR_NIVEL)
    g['Acción'] = g['NIVEL'].map(lambda n: C.NIVELES[n][1])
    g['Nivel'] = 'N' + g['NIVEL'].astype(str)
    y = alt.Y('CATEGORIA:N', sort='-x', title=None,
              axis=alt.Axis(labelOverlap=False, labelLimit=190, labelFontSize=11.5,
                            labelColor=INK2, domain=False, ticks=False))
    barra = (alt.Chart(g).mark_bar(cornerRadius=4, size=15)
             .encode(x=alt.X('Delitos:Q', title='delitos en rojo'), y=y,
                     color=alt.Color('color:N', scale=None, legend=None),
                     tooltip=['CATEGORIA', 'Delitos', 'Nivel', 'Acción']))
    txt = (alt.Chart(g).mark_text(align='left', dx=6, fontSize=11.5, font=FONT,
                                  fontWeight='bold')
           .encode(x='Delitos:Q', y=y, text='Delitos:Q',
                   color=alt.Color('color:N', scale=None, legend=None)))
    return _base(barra + txt, max(215, 25 * len(g) + 45))


# --------------------------------------------------------- delitos por año
def por_anio(df_d: pd.DataFrame):
    if not len(df_d) or df_d['ANIO'].notna().sum() == 0:
        return None
    g = (df_d.dropna(subset=['ANIO']).assign(ANIO=lambda x: x['ANIO'].astype(int))
         .groupby('ANIO').size().reset_index(name='Delitos'))
    g['Vigencia'] = g['ANIO'].map(
        lambda a: 'Vigente 2023+' if a >= C.ANIO_VIGENCIA else 'Anterior a 2023')
    x = alt.X('ANIO:O', title=None, axis=alt.Axis(labelAngle=-50, labelFontSize=10.5,
                                                  domain=False, ticks=False))
    col = alt.Color('Vigencia:N', title=None,
                    scale=alt.Scale(domain=['Anterior a 2023', 'Vigente 2023+'],
                                    range=[C.BRAND, C.CRIT]),
                    legend=alt.Legend(orient='top', direction='horizontal', offset=2))
    barra = (alt.Chart(g).mark_bar(cornerRadius=3, size=17)
             .encode(x=x, y=alt.Y('Delitos:Q', title='delitos',
                                  axis=alt.Axis(tickCount=5)),
                     color=col, tooltip=['ANIO', 'Delitos', 'Vigencia']))
    txt = (alt.Chart(g[g['Delitos'] >= 5]).mark_text(dy=-7, fontSize=10.5, font=FONT,
                                                     fontWeight='bold', color=INK2)
           .encode(x=x, y='Delitos:Q', text='Delitos:Q'))
    return _base(barra + txt, 250)


# ---------------------------------------------------------- composición
def composicion(df: pd.DataFrame, campo: str):
    """Barras apiladas verticales: composición de veredictos por labor / tipo / cuadrilla."""
    g = df.groupby([campo, 'VEREDICTO']).size().reset_index(name='Personas')
    if not len(g):
        return None
    tot = df.groupby(campo).size()
    noapto = (df[df['VEREDICTO'] == 'NO APTO'].groupby(campo).size()
              .reindex(tot.index, fill_value=0))
    orden = noapto.sort_values(ascending=False).index.tolist()
    g['_ord'] = g['VEREDICTO'].map({v: i for i, v in enumerate(C.VEREDICTOS)})
    g['Total'] = g[campo].map(tot)
    g['Porcentaje'] = (g['Personas'] / g['Total'])
    n = g[campo].nunique()
    x = alt.X(f'{campo}:N', sort=orden, title=None,
              scale=alt.Scale(paddingInner=0.5 if n <= 3 else 0.28, paddingOuter=0.3),
              axis=alt.Axis(labelAngle=0 if n <= 3 else -22, labelLimit=160,
                            labelFontSize=12, labelFontWeight='bold', labelColor=INK2,
                            domain=False, ticks=False))
    col = alt.Color('VEREDICTO:N', title=None,
                    scale=alt.Scale(domain=C.VEREDICTOS,
                                    range=[C.COLOR[v] for v in C.VEREDICTOS]),
                    legend=alt.Legend(orient='bottom', columns=3, labelLimit=180))
    orden_pila = alt.Order('_ord:Q', sort='ascending')
    tip = [alt.Tooltip(f'{campo}:N', title=campo.replace('_', ' ').title()), 'VEREDICTO',
           'Personas', 'Total', alt.Tooltip('Porcentaje:Q', format='.0%')]
    barra = (alt.Chart(g).mark_bar(cornerRadius=2)
             .encode(x=x, y=alt.Y('Personas:Q', stack='zero', title='personas'),
                     color=col, order=orden_pila, tooltip=tip))
    etq = (alt.Chart(g[g['Personas'] >= 4]).mark_text(fontSize=11, font=FONT,
                                                      fontWeight='bold', color='white')
           .encode(x=x, y=alt.Y('Personas:Q', stack='zero'), detail='VEREDICTO:N',
                   order=orden_pila, text='Personas:Q'))
    return _base(barra + etq, 340)


# ----------------------------------------------- índice contra antigüedad
def dispersion_indice(df_p: pd.DataFrame):
    d = df_p[df_p['N_DELITOS'] > 0].copy()
    if not len(d):
        return None
    d['Último año'] = d['ANIO_MAX'].fillna(0).astype(int)
    banda = pd.DataFrame([{'y0': 75, 'y1': 100}])
    fondo = (alt.Chart(banda).mark_rect(color=C.CRIT, opacity=.06)
             .encode(y='y0:Q', y2='y1:Q'))
    puntos = (alt.Chart(d).mark_circle(opacity=.85, stroke='white', strokeWidth=1.2)
              .encode(x=alt.X('Último año:Q', title='año del registro más reciente',
                              scale=alt.Scale(zero=False, nice=True),
                              axis=alt.Axis(format='d')),
                      y=alt.Y('INDICE:Q', title='índice de riesgo (0-100)',
                              scale=alt.Scale(domain=[0, 105])),
                      size=alt.Size('N_DELITOS:Q', title='delitos',
                                    scale=alt.Scale(range=[50, 480]),
                                    legend=alt.Legend(orient='right')),
                      color=alt.Color('VEREDICTO:N', title=None,
                                      scale=alt.Scale(domain=C.VEREDICTOS,
                                                      range=[C.COLOR[v] for v in C.VEREDICTOS]),
                                      legend=alt.Legend(orient='bottom', columns=3)),
                      tooltip=[alt.Tooltip(C.COL_NOMBRE, title='Colaborador'), 'DNI', 'LABOR',
                               'NIVEL_MATRIZ', 'INDICE', 'N_DELITOS', 'Último año', 'VEREDICTO']))
    return _base(fondo + puntos, 360)


# ------------------------------------------------- planilla Prize
def planilla(df_p: pd.DataFrame, campo: str = "AREA", top: int = 12):
    """Personas por área/cargo de planilla, teñidas por veredicto."""
    if campo not in df_p.columns or not len(df_p):
        return None
    d = df_p[[campo, 'VEREDICTO']].copy()
    d[campo] = d[campo].replace("", "SIN DATO").fillna("SIN DATO")
    grandes = d[campo].value_counts().head(top).index.tolist()
    d = d[d[campo].isin(grandes)]
    g = d.groupby([campo, 'VEREDICTO']).size().reset_index(name='Personas')
    if not len(g):
        return None
    g['_ord'] = g['VEREDICTO'].map({v: i for i, v in enumerate(C.VEREDICTOS)})
    tot = d[campo].value_counts()
    g['Total'] = g[campo].map(tot)
    y = alt.Y(f'{campo}:N', sort=grandes, title=None,
              axis=alt.Axis(labelLimit=210, labelFontSize=11.5, labelColor=INK2,
                            domain=False, ticks=False))
    col = alt.Color('VEREDICTO:N', title=None,
                    scale=alt.Scale(domain=C.VEREDICTOS,
                                    range=[C.COLOR[v] for v in C.VEREDICTOS]),
                    legend=alt.Legend(orient='bottom', columns=2, labelLimit=220,
                                      symbolSize=90, labelFontSize=11))
    barra = (alt.Chart(g).mark_bar(cornerRadius=3, height=16)
             .encode(x=alt.X('Personas:Q', stack='zero', title='personas'), y=y, color=col,
                     order=alt.Order('_ord:Q', sort='ascending'),
                     tooltip=[alt.Tooltip(f'{campo}:N', title=campo.title()),
                              'VEREDICTO', 'Personas', 'Total']))
    return _base(barra, max(230, 26 * len(grandes) + 70))
