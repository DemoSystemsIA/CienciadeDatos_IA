# -*- coding: utf-8 -*-
"""
Elección de la carpeta de trabajo.

La app ya no depende de una ruta fija: el usuario elige la carpeta que quiera,
desde un explorador nativo de Windows, desde el historial de carpetas usadas
o escribiendo la ruta a mano.

Piezas:
  · elegir_carpeta_nativa()  -> abre el explorador del sistema operativo
  · diagnostico(ruta)        -> qué hay dentro (cuadrillas, Excel, PDF)
  · recientes / recordar     -> historial persistente entre sesiones
  · candidatas(base)         -> subcarpetas que parecen una raíz válida
"""
from __future__ import annotations

import glob
import json
import os
import subprocess
import sys

from . import config as C

# ------------------------------------------------------- historial en disco
DIR_ESTADO = os.path.join(os.path.expanduser("~"), ".filtro_antecedentes")
ARCHIVO_RECIENTES = os.path.join(DIR_ESTADO, "carpetas_recientes.json")
MAX_RECIENTES = 12


def _normalizar(ruta: str) -> str:
    return os.path.normpath(os.path.abspath(os.path.expanduser((ruta or "").strip().strip('"'))))


def recientes() -> list[str]:
    """Carpetas usadas antes, de la más reciente a la más antigua. Solo las que existen."""
    try:
        with open(ARCHIVO_RECIENTES, "r", encoding="utf-8") as f:
            datos = json.load(f)
    except Exception:
        return []
    if not isinstance(datos, list):
        return []
    fuera, vistas = [], set()
    for r in datos:
        if not isinstance(r, str):
            continue
        r = _normalizar(r)
        if r.lower() in vistas or not os.path.isdir(r):
            continue
        vistas.add(r.lower())
        fuera.append(r)
    return fuera[:MAX_RECIENTES]


def recordar(ruta: str) -> None:
    """Pone la carpeta al principio del historial."""
    ruta = _normalizar(ruta)
    if not os.path.isdir(ruta):
        return
    lista = [r for r in recientes() if r.lower() != ruta.lower()]
    lista.insert(0, ruta)
    try:
        os.makedirs(DIR_ESTADO, exist_ok=True)
        with open(ARCHIVO_RECIENTES, "w", encoding="utf-8") as f:
            json.dump(lista[:MAX_RECIENTES], f, ensure_ascii=False, indent=1)
    except OSError:
        pass


def olvidar(ruta: str | None = None) -> None:
    """Borra una carpeta del historial, o el historial entero si no se indica ninguna."""
    if ruta is None:
        lista = []
    else:
        ruta = _normalizar(ruta)
        lista = [r for r in recientes() if r.lower() != ruta.lower()]
    try:
        os.makedirs(DIR_ESTADO, exist_ok=True)
        with open(ARCHIVO_RECIENTES, "w", encoding="utf-8") as f:
            json.dump(lista, f, ensure_ascii=False, indent=1)
    except OSError:
        pass


# ------------------------------------------------- explorador del sistema
# El diálogo se abre en un proceso aparte: Streamlit ejecuta el script en un
# hilo secundario y Tkinter no es fiable fuera del hilo principal.
_SCRIPT_DIALOGO = r"""
import sys
try:
    import tkinter as tk
    from tkinter import filedialog
except Exception as e:
    sys.stderr.write("SIN_TKINTER: %s" % e)
    raise SystemExit(2)
inicial = sys.argv[1] if len(sys.argv) > 1 else ""
raiz = tk.Tk()
raiz.withdraw()
raiz.update()
try:
    raiz.attributes("-topmost", True)
except Exception:
    pass
ruta = filedialog.askdirectory(
    title="Elige la carpeta con las cuadrillas (cada subcarpeta = una cuadrilla)",
    initialdir=inicial or None, mustexist=True)
try:
    raiz.destroy()
except Exception:
    pass
sys.stdout.write(ruta or "")
"""


def elegir_carpeta_nativa(inicial: str = "", timeout: int = 600):
    """
    Abre el explorador de carpetas del sistema operativo.

    Devuelve (ruta, error):
      · (ruta, None)  el usuario eligió una carpeta
      · (None, None)  el usuario canceló
      · (None, texto) no se pudo abrir el diálogo (servidor sin escritorio, etc.)
    """
    inicial = _normalizar(inicial) if inicial else ""
    if inicial and not os.path.isdir(inicial):
        inicial = ""
    banderas = {}
    if os.name == "nt":
        banderas["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        proc = subprocess.run([sys.executable, "-c", _SCRIPT_DIALOGO, inicial],
                              capture_output=True, text=True, timeout=timeout, **banderas)
    except subprocess.TimeoutExpired:
        return None, "El diálogo quedó abierto demasiado tiempo y se canceló."
    except Exception as e:
        return None, f"No se pudo abrir el explorador de carpetas: {e}"

    if proc.returncode == 2 or "SIN_TKINTER" in (proc.stderr or ""):
        return None, ("Este equipo no tiene Tkinter disponible, así que no se puede abrir el "
                      "explorador. Usa el historial o pega la ruta a mano.")
    if proc.returncode != 0:
        detalle = (proc.stderr or "").strip().splitlines()[-1:] or [""]
        return None, f"El explorador de carpetas falló. {detalle[0]}"

    ruta = (proc.stdout or "").strip()
    if not ruta:
        return None, None
    return _normalizar(ruta), None


def hay_explorador() -> bool:
    """¿Se puede abrir el diálogo nativo en este equipo?"""
    try:
        import tkinter  # noqa: F401
        return True
    except Exception:
        return False


# ------------------------------------------------------------ diagnóstico
def _mide(carpeta: str) -> tuple[int, int]:
    """(nº de Excel de resumen, nº de PDF) dentro de UNA carpeta de cuadrilla."""
    ex = [p for p in glob.glob(os.path.join(carpeta, C.PATRON_EXCEL))
          if not os.path.basename(p).startswith("~$")]
    pdfs = glob.glob(os.path.join(carpeta, C.SUBCARPETA_PDF, C.PATRON_PDF))
    return len(ex), len(pdfs)


def diagnostico(ruta: str) -> dict:
    """
    Mira la carpeta sin leer nada pesado y dice si sirve como raíz.

    Devuelve dict con:
      existe, es_raiz, cuadrillas (nombres), n_excels, n_pdfs,
      sugerencia (ruta alternativa mejor: el padre o una subcarpeta), motivo
    """
    fuera = dict(ruta="", existe=False, es_raiz=False, cuadrillas=[], n_excels=0,
                 n_pdfs=0, sugerencia=None, motivo="")
    if not (ruta or "").strip():
        fuera["motivo"] = "Indica una carpeta."
        return fuera
    ruta = _normalizar(ruta)
    fuera["ruta"] = ruta
    if not os.path.isdir(ruta):
        fuera["motivo"] = "La carpeta no existe o no es accesible."
        return fuera
    fuera["existe"] = True

    try:
        hijos = sorted(n for n in os.listdir(ruta)
                       if os.path.isdir(os.path.join(ruta, n)) and not n.startswith("."))
    except OSError as e:
        fuera["motivo"] = f"No se pudo leer la carpeta: {e}"
        return fuera

    cuadrillas, n_ex, n_pdf = [], 0, 0
    for nombre in hijos:
        e, p = _mide(os.path.join(ruta, nombre))
        if e or p:
            cuadrillas.append(nombre)
            n_ex += e
            n_pdf += p
    fuera.update(cuadrillas=cuadrillas, n_excels=n_ex, n_pdfs=n_pdf,
                 es_raiz=bool(n_ex))

    if fuera["es_raiz"]:
        fuera["motivo"] = (f"{len(cuadrillas)} cuadrilla(s) · {n_ex} Excel · {n_pdf} PDF")
        return fuera

    # ¿El usuario eligió una carpeta de cuadrilla en lugar de la raíz?
    propios_ex, propios_pdf = _mide(ruta)
    if propios_ex or propios_pdf:
        padre = os.path.dirname(ruta)
        d_padre = _mide_raiz_rapido(padre)
        if d_padre:
            fuera["sugerencia"] = padre
            fuera["motivo"] = ("Esta carpeta es una cuadrilla, no la raíz. "
                               f"La raíz parece ser «{os.path.basename(padre) or padre}».")
            return fuera

    # ¿La raíz está un nivel más abajo?
    for nombre in hijos:
        sub = os.path.join(ruta, nombre)
        if _mide_raiz_rapido(sub):
            fuera["sugerencia"] = sub
            fuera["motivo"] = f"Aquí no hay cuadrillas, pero «{nombre}» sí las tiene."
            return fuera

    fuera["motivo"] = (f"No se encontró ningún «{C.PATRON_EXCEL}» en las subcarpetas. "
                       "Elige la carpeta que CONTIENE a las cuadrillas.")
    return fuera


def _mide_raiz_rapido(ruta: str) -> bool:
    """¿Esta carpeta tiene al menos una subcarpeta de cuadrilla con Excel?"""
    try:
        hijos = [n for n in os.listdir(ruta)
                 if os.path.isdir(os.path.join(ruta, n)) and not n.startswith(".")]
    except OSError:
        return False
    for n in hijos[:200]:
        ex, _ = _mide(os.path.join(ruta, n))
        if ex:
            return True
    return False


def candidatas(base: str, limite: int = 40) -> list[str]:
    """Subcarpetas de `base` que parecen una raíz válida (para ofrecerlas en un desplegable)."""
    base = _normalizar(base)
    if not os.path.isdir(base):
        return []
    fuera = []
    try:
        hijos = sorted(n for n in os.listdir(base)
                       if os.path.isdir(os.path.join(base, n)) and not n.startswith("."))
    except OSError:
        return []
    for n in hijos[:limite]:
        sub = os.path.join(base, n)
        if _mide_raiz_rapido(sub):
            fuera.append(sub)
    return fuera


def etiqueta(ruta: str, ancho: int = 46) -> str:
    """Ruta acortada para desplegables: C:\\…\\PADRE\\CARPETA."""
    ruta = _normalizar(ruta)
    if len(ruta) <= ancho:
        return ruta
    partes = [p for p in ruta.split(os.sep) if p]
    if len(partes) <= 3:
        return ruta
    cabeza = partes[0] if os.name == "nt" else os.sep + partes[0]
    return os.sep.join([cabeza, "…"] + partes[-2:])
