# -*- coding: utf-8 -*-
"""
Genera resultado_final.xlsx sin abrir el tablero.

    python generar_excel.py                  -> usa C:\\FILTER
    python generar_excel.py "D:\\otra\\ruta"  -> usa esa carpeta
    python generar_excel.py C:\\FILTER --validar referencia.xlsx
"""
import sys
import os
import argparse

from filtro import config as C
from filtro.loader import construir
from filtro.excel_export import construir_excel
from filtro.validate import validar


def main():
    ap = argparse.ArgumentParser(description="Filtro de antecedentes Prize / Aquanqa")
    ap.add_argument("raiz", nargs="?", default=C.CARPETA_POR_DEFECTO)
    ap.add_argument("--salida", default=None, help="ruta del .xlsx de salida")
    ap.add_argument("--validar", default=None, help="archivo de referencia para comparar")
    a = ap.parse_args()

    def prog(i, total, nombre):
        print(f"\r  PDF {i}/{total}  {nombre[:48]:<48}", end="", flush=True)

    print(f"Carpeta: {a.raiz}")
    df_p, df_d, meta = construir(a.raiz, progreso=prog)
    print()
    print(f"  {meta['filas_excel']} filas -> {meta['dni_unicos']} DNI únicos "
          f"({meta['dni_repetidos']} repetidos)")
    print(f"  {meta['con_pdf']} con reporte / {meta['sin_pdf']} sin reporte · "
          f"{len(df_d)} delitos en rojo")
    for v, n in df_p['VEREDICTO'].value_counts().items():
        print(f"    {v:<24} {n}")

    destino = a.salida or os.path.join(a.raiz, C.NOMBRE_SALIDA)
    datos = construir_excel(df_p, df_d, meta)
    try:
        with open(destino, "wb") as f:
            f.write(datos)
        print(f"\nGenerado: {destino}")
    except PermissionError:
        alt = destino.replace(".xlsx", "_nuevo.xlsx")
        with open(alt, "wb") as f:
            f.write(datos)
        print(f"\n{destino} está abierto en Excel. Se guardó como: {alt}")

    if a.validar:
        rep = validar(df_p, a.validar)
        print(f"\nValidación contra {a.validar}")
        if rep['filtro']:
            print(f"  FILTRO          {rep['filtro']['ok']}/{rep['filtro']['total']}")
        if rep['resumen']:
            print(f"  Resumen_Persona {rep['resumen']['ok']}/{rep['resumen']['total']}")
        if rep['detalles']:
            print(f"  {len(rep['detalles'])} discrepancias:")
            for d in rep['detalles'][:20]:
                print(f"    {d['DNI']} · {d['CAMPO']}\n       esperado: {d['ESPERADO'][:110]}"
                      f"\n       obtenido: {d['OBTENIDO'][:110]}")
        else:
            print("  Cero discrepancias.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
