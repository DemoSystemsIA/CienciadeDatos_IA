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
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile

from . import config as C
from . import maestro as M

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


# =====================================================================
#  MODO WEB  (Streamlit Cloud u otro servidor)
# ---------------------------------------------------------------------
#  Un servidor web NO puede leer el disco de quien lo visita: el proceso
#  corre en otra máquina. Ahí la carpeta del usuario llega subida desde
#  el navegador y se trabaja sobre una copia temporal, propia de esa
#  sesión y borrada al terminar.
# =====================================================================
RAIZ_TEMP = os.path.join(tempfile.gettempdir(), "filtro_web")
VIDA_TEMP_HORAS = 6


def modo_servidor() -> bool:
    """
    ¿Estamos en un servidor (Streamlit Cloud, Docker, VM sin escritorio)?

    Se puede forzar con la variable de entorno FILTRO_MODO=local | web.
    """
    forzado = (os.environ.get("FILTRO_MODO") or "").strip().lower()
    if forzado in ("local", "escritorio"):
        return False
    if forzado in ("web", "nube", "servidor", "cloud"):
        return True
    if os.path.isdir("/mount/src"):                 # Streamlit Community Cloud
        return True
    if os.environ.get("STREAMLIT_SERVER_HEADLESS", "").lower() == "true" and os.name != "nt":
        return True
    if os.name == "nt" or sys.platform == "darwin":  # Windows / macOS: escritorio
        return False
    return not (hay_explorador() and os.environ.get("DISPLAY"))


def carpeta_sesion(token: str) -> str:
    """Carpeta temporal privada de una sesión web."""
    seguro = re.sub(r'[^A-Za-z0-9_-]', '', str(token))[:40] or "sesion"
    destino = os.path.join(RAIZ_TEMP, seguro)
    os.makedirs(destino, exist_ok=True)
    return destino


def limpiar_temporales(horas: int = VIDA_TEMP_HORAS) -> int:
    """Borra las carpetas subidas hace más de `horas`. Devuelve cuántas borró."""
    if not os.path.isdir(RAIZ_TEMP):
        return 0
    corte, n = time.time() - horas * 3600, 0
    for nombre in os.listdir(RAIZ_TEMP):
        ruta = os.path.join(RAIZ_TEMP, nombre)
        try:
            if os.path.isdir(ruta) and os.path.getmtime(ruta) < corte:
                shutil.rmtree(ruta, ignore_errors=True)
                n += 1
        except OSError:
            pass
    return n


def borrar_sesion(destino: str) -> None:
    """Borra del servidor todo lo que subió esta sesión."""
    destino = _normalizar(destino)
    if destino.startswith(_normalizar(RAIZ_TEMP)) and os.path.isdir(destino):
        shutil.rmtree(destino, ignore_errors=True)


def _destino_seguro(base: str, nombre: str) -> str | None:
    """Evita el «zip slip»: nada puede escribirse fuera de `base`."""
    nombre = nombre.replace("\\", "/")
    if nombre.startswith("/") or ".." in nombre.split("/"):
        return None
    ruta = os.path.normpath(os.path.join(base, *[p for p in nombre.split("/") if p]))
    if not os.path.normpath(ruta).startswith(os.path.normpath(base)):
        return None
    return ruta


EXT_PERMITIDAS = (".xlsx", ".xlsm", ".pdf", ".csv")


def _guardar_maestro(datos: bytes, destino: str, nombre: str) -> None:
    """Deja el export de funcionarios donde el tablero lo busca siempre."""
    ext = os.path.splitext(nombre)[1].lower() or ".csv"
    carpeta = os.path.join(destino, ".cache_filtro")
    os.makedirs(carpeta, exist_ok=True)
    with open(os.path.join(carpeta, f"maestro_prize{ext}"), "wb") as f:
        f.write(datos)


def extraer_zip(archivo, destino: str) -> dict:
    """
    Extrae el .zip que subió el usuario respetando su estructura de carpetas.
    Solo saca Excel y PDF; ignora todo lo demás.
    """
    shutil.rmtree(destino, ignore_errors=True)
    os.makedirs(destino, exist_ok=True)
    n_ex = n_pdf = n_ign = n_mae = 0
    try:
        with zipfile.ZipFile(archivo) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                base = os.path.basename(info.filename)
                if base.startswith("~$") or base.startswith("."):
                    continue
                ext = os.path.splitext(base)[1].lower()
                if ext not in EXT_PERMITIDAS:
                    n_ign += 1
                    continue
                if M.parece_maestro(base):          # export de funcionarios
                    with z.open(info) as origen:
                        _guardar_maestro(origen.read(), destino, base)
                    n_mae += 1
                    continue
                ruta = _destino_seguro(destino, info.filename)
                if not ruta:
                    n_ign += 1
                    continue
                os.makedirs(os.path.dirname(ruta), exist_ok=True)
                with z.open(info) as origen, open(ruta, "wb") as f:
                    shutil.copyfileobj(origen, f)
                n_ex += ext != ".pdf"
                n_pdf += ext == ".pdf"
    except zipfile.BadZipFile:
        return dict(ok=False, motivo="El archivo no es un .zip válido.",
                    excels=0, pdfs=0, ignorados=0, maestros=0, raiz=destino)
    if not n_ex:
        return dict(ok=False, excels=0, pdfs=n_pdf, ignorados=n_ign, maestros=n_mae,
                    raiz=destino, motivo=f"El .zip no trae ningún «{C.PATRON_EXCEL}».")
    raiz = _raiz_dentro(destino)
    if n_mae and raiz != destino:      # el maestro va donde esté la raíz real
        _mover_maestro(destino, raiz)
    return dict(ok=True, excels=n_ex, pdfs=n_pdf, ignorados=n_ign, maestros=n_mae,
                raiz=raiz, motivo="")


def _mover_maestro(origen: str, destino: str) -> None:
    for nombre in M.GUARDADOS:
        a = os.path.join(origen, ".cache_filtro", nombre)
        if os.path.isfile(a):
            os.makedirs(os.path.join(destino, ".cache_filtro"), exist_ok=True)
            shutil.move(a, os.path.join(destino, ".cache_filtro", nombre))


def _raiz_dentro(base: str) -> str:
    """
    Si al comprimir se envolvió todo en una carpeta («FILTER/ESTIBAS01/…»),
    baja hasta el nivel donde están de verdad las cuadrillas.
    """
    actual = base
    for _ in range(4):
        if _mide_raiz_rapido(actual):
            return actual
        try:
            hijos = [n for n in os.listdir(actual)
                     if os.path.isdir(os.path.join(actual, n)) and not n.startswith(".")]
        except OSError:
            break
        if len(hijos) != 1:
            break
        actual = os.path.join(actual, hijos[0])
    return actual if _mide_raiz_rapido(actual) else base


RE_CUADRILLA = re.compile(r'Resumen_NEW_VIP_(.+)', re.I)


def guardar_sueltos(archivos, destino: str) -> dict:
    """
    Alternativa al .zip: el usuario selecciona los archivos a mano.

    Como el navegador no manda las carpetas, la cuadrilla se deduce del nombre
    del Excel (Resumen_NEW_VIP_<CUADRILLA>.xlsx) y todos los PDF van a una
    carpeta «Adjuntos» común — el cruce PDF -> persona es por DNI, no por
    carpeta, así que el resultado es el mismo.
    """
    shutil.rmtree(destino, ignore_errors=True)
    os.makedirs(destino, exist_ok=True)
    comun = os.path.join(destino, C.SUBCARPETA_PDF)
    os.makedirs(comun, exist_ok=True)
    n_ex = n_pdf = n_ign = n_mae = 0
    for f in archivos or []:
        base = os.path.basename(getattr(f, "name", "") or "")
        ext = os.path.splitext(base)[1].lower()
        datos = f.getbuffer() if hasattr(f, "getbuffer") else f.read()
        if M.parece_maestro(base):              # export de funcionarios
            _guardar_maestro(bytes(datos), destino, base)
            n_mae += 1
        elif ext == ".pdf":
            with open(os.path.join(comun, base), "wb") as s:
                s.write(datos)
            n_pdf += 1
        elif ext in (".xlsx", ".xlsm") and not base.startswith("~$"):
            m = RE_CUADRILLA.match(os.path.splitext(base)[0])
            cuad = (m.group(1) if m else os.path.splitext(base)[0]).strip() or "CUADRILLA"
            cuad = re.sub(r'[\\/:*?"<>|]', "_", cuad)
            carpeta = os.path.join(destino, cuad)
            os.makedirs(carpeta, exist_ok=True)
            nombre = base if RE_CUADRILLA.match(os.path.splitext(base)[0]) \
                else f"Resumen_NEW_VIP_{cuad}.xlsx"
            with open(os.path.join(carpeta, nombre), "wb") as s:
                s.write(datos)
            n_ex += 1
        else:
            n_ign += 1
    if not n_ex:
        return dict(ok=False, excels=0, pdfs=n_pdf, ignorados=n_ign, maestros=n_mae,
                    raiz=destino,
                    motivo=f"Falta el «{C.PATRON_EXCEL}»: sin él no hay padrón que leer.")
    return dict(ok=True, excels=n_ex, pdfs=n_pdf, ignorados=n_ign, maestros=n_mae,
                raiz=destino, motivo="")


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
