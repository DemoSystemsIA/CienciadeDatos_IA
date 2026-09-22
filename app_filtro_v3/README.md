# Filtro de Antecedentes · Prize / Aquanqa

Tablero Streamlit que apunta a **la carpeta que elija el usuario** (botón *Examinar…* en la
barra lateral; ya no hay ninguna ruta fija), cruza los DNI de
todos los `Resumen_NEW_VIP_*.xlsx` contra los reportes PDF de cada subcarpeta `Adjuntos`,
extrae **solo los delitos pintados en rojo**, los clasifica contra la matriz de criticidad
de Prize y genera `resultado_final.xlsx`.

Reproduce al 100% el archivo de referencia `Resumen_NEW_VIP_Filtro_3.xlsx`:
**30/30** valores de `FILTRO` y **360/360** celdas de `Resumen_Persona` idénticas.

---

## Dos modos: escritorio y web

La app detecta sola dónde corre.

| | Escritorio (tu PC) | Web (Streamlit Cloud, servidor) |
|---|---|---|
| Origen de los datos | Cualquier carpeta de tu disco | Una carpeta que **subes desde el navegador** |
| Cómo se elige | Botón **📂 Examinar carpeta…**, historial, o ruta a mano | Arrastrar un `.zip`, o seleccionar los archivos sueltos |
| Dónde viven los datos | Solo en tu PC | Copia temporal en el servidor, borrada al cerrar |

**Por qué la web no puede leer `C:\FILTER`:** el proceso corre en otra máquina (un
Linux de Streamlit), no en tu PC. Ningún servidor web puede abrir el disco de quien lo
visita — el navegador no lo permite, y es la razón del error
`No existe la carpeta /mount/src/<repo>/C:\FILTER`. Por eso en la nube la carpeta se
sube, y por eso el botón *Examinar* (que usa el explorador de Windows) solo aparece en
escritorio.

La detección se puede forzar con la variable de entorno `FILTRO_MODO=local` o
`FILTRO_MODO=web`.

### Subir la carpeta en la versión web

Tres formas, en la barra lateral:

| Modo | Qué hace | Qué lee |
|---|---|---|
| **📁 Carpeta** | Abre el selector de **carpetas** del navegador: eliges tu carpeta y se manda su contenido. Sin comprimir nada | `.xlsx` `.xlsm` `.pdf` `.csv` — **los `.zip` se ignoran** |
| **🗜️ .zip** | Subes la carpeta comprimida; conserva la estructura tal cual | solo `.zip` |
| **📄 Archivos** | Seleccionas los archivos a mano (Excel, PDF y el maestro) | `.xlsx` `.xlsm` `.pdf` `.csv` |

**Por qué el modo Carpeta salta los `.zip`:** una carpeta de trabajo real suele tener
comprimidos sueltos (respaldos, envíos antiguos, el resultado de una corrida anterior)
que no son el padrón. Subirlos sería lento y no aportaría nada, así que se descartan
**en el navegador, antes de subir** — un filtro en fase de captura reemplaza la lista de
archivos del input, y la consola deja constancia de cuántos saltó. Por si el navegador
no lo soportara, `guardar_sueltos()` los vuelve a descartar en el servidor y el tablero
avisa. Para leer un comprimido está el modo **🗜️ .zip**, que sí lo abre.

El modo *Carpeta* añade el atributo `webkitdirectory` al cargador de Streamlit desde el
propio navegador (`selector_de_carpeta_js()` en `app.py`). El navegador no manda las
subcarpetas, así que la cuadrilla se deduce del nombre de cada Excel y los PDF van a una
carpeta `Adjuntos` común: el cruce PDF → persona es por DNI, de modo que el resultado es
idéntico. Si el navegador no soportara el selector de carpetas, quedan el `.zip` y los
archivos sueltos.

Si el `.zip` envuelve todo en una carpeta (`FILTER/ESTIBAS01/...`), la app baja sola
hasta el nivel correcto. Solo se extraen `.xlsx` y `.pdf`; cualquier otra cosa se
descarta, y las rutas que apunten fuera de la carpeta de destino (*zip slip*) se
rechazan.

**Privacidad.** Cada sesión trabaja en su propia carpeta temporal y no ve la de nadie
más; el botón *Borrar mis datos del servidor* la elimina en el acto y, si no, se limpia
sola a las 6 horas. Aun así, una app pública en Streamlit Cloud la puede abrir cualquiera
con el enlace: para datos reales conviene un repositorio privado y restringir los correos
en **Settings -> Sharing** de Streamlit Cloud. El límite de subida es `MAX_SUBIDA_MB` en
`filtro/config.py` y `maxUploadSize` en `.streamlit/config.toml` (deben coincidir).

---

## Planilla Prize: quién es cada DNI

El tablero cruza el padrón contra el **maestro de funcionarios** (el export de qbiz, que
trae todos los campos dentro de una columna `payload` en JSON). El cruce es por DNI,
comparando solo dígitos y sin ceros a la izquierda.

- Un DNI con varios contratos deja **una sola fila**: la vigente y, entre ellas, la de
  modificación más reciente. `N_CONTRATOS` dice cuántas había.
- Un DNI que no está en el maestro se marca **`NO PERTENECE A PRIZE`** — normalmente es
  personal de contrata, o un DNI mal escrito en el Excel de la cuadrilla.
- Campos que se añaden: empresa, código de funcionario, área, cargo, centro de costo,
  régimen, tipo de trabajador, planilla, fechas de ingreso y cese, antigüedad y situación
  (`ACTIVO EN PLANILLA` / `CESADO`).
- Si el Excel de la cuadrilla ya traía una columna con el mismo nombre (CARGO, ÁREA…),
  la original se conserva renombrada a `<COLUMNA>_ORIGEN`. Nunca se pisa un dato de
  origen sin dejar rastro.

**De dónde sale el archivo:** se deja en la carpeta de trabajo (vale cualquier nombre con
`funcionario`, `qbiz`, `maestro` o `planilla`) y el tablero lo detecta solo; o se sube
desde la barra lateral, en *Maestro de planilla*. En la versión web basta con que venga
dentro de la carpeta o del `.zip`.

Alimenta la sección **Planilla Prize**, la franja de totales de la cabecera, la columna
*Planilla Prize* del padrón, los filtros por área / empresa / situación y la sección 8
del Excel.

---

## Tema claro y oscuro

Botón ☀️/🌙 arriba en la barra lateral. Cambia la paleta completa: interfaz, tarjetas,
tablas y **también los gráficos** (ejes, rejilla y colores se recalculan). Los acentos no
son los mismos en los dos temas: sobre fondo oscuro suben en luminosidad para mantener
contraste. Vive en `config.TEMAS`, `ui.TEMAS` y `charts.TEMAS`, cada uno con su
`aplicar_tema()`.

---

## El Excel: cuatro hojas, sin repetir nada

| Hoja | Qué trae |
|---|---|
| `Padron` | Una fila por persona, con todo: filtro, matriz, índice y los campos de planilla |
| `Delitos` | Una fila por registro en rojo |
| `Resumen` | Los conteos: cobertura, decisión, riesgo, categorías, señales, cortes por labor, matriz de criticidad y cruce con planilla |
| `Criterio_y_Metodo` | Las reglas N1-N6 en orden y la trazabilidad del proceso |

Antes eran siete y tres pares repetían los mismos datos: `Hoja1` y `Resumen_Persona`
tenían el mismo padrón; `Matriz_Criticidad` repetía conteos que ya estaban en
`Estadisticas`; y `Fuentes_y_Metodo` continuaba lo de `Criterio_N1_N6`. Ahora cada dato
vive en un solo sitio.

---

## Publicar en Streamlit Cloud

La app se despliega desde un repositorio de GitHub, así que **todo el código tiene que
estar en el repositorio**, no solo `app.py`. Publicar `app.py` nuevo con los módulos de
`filtro/` viejos es el error más fácil de cometer y antes reventaba con un
`AttributeError` incomprensible.

Desde la versión 3.2 cada módulo declara su `VERSION` y `app.py` las compara al arrancar:
si algo quedó desfasado, la app dice **qué archivo** hay que actualizar en vez de fallar.

En Windows:

```
publicar_en_repo.bat "C:\ruta\al\repo\cienciadedatos_ia\app_filtro_v3"
```

Copia los `.py`, `requirements.txt`, `.streamlit/config.toml` y el README; borra las
cachés del destino y comprueba que no falte ninguno de los archivos de `filtro/`. Después:

```
git rm -r --cached --ignore-unmatch "app_filtro_v3/filtro/__pycache__"
git add -A
git commit -m "..."
git push
```

> **`__pycache__` versionado.** Si los `.pyc` llegaron alguna vez al repositorio, Python
> puede acabar importando código viejo. El `.gitignore` ya los excluye, pero lo que se
> subió antes sigue ahí hasta que se quita con `git rm -r --cached`.

Los datos (carpetas de cuadrillas, PDF, el maestro de funcionarios) **no van al
repositorio**: en la web se suben desde el navegador en cada sesión.

---

## Instalación (Windows)

Doble clic en **`run_windows.bat
publicar_en_repo.bat    copia el codigo al repositorio de Git, sin cachés ni datos`**. La primera vez crea el entorno virtual e instala
las dependencias; después solo levanta el tablero. Necesita Python 3.9 o superior
([python.org](https://www.python.org/downloads/) — marca *Add Python to PATH* al instalar).

A mano:

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

Se abre en `http://localhost:8501`.

---

## Estructura que espera

```
C:\FILTER\
├── ESTIBAS01\
│   ├── Resumen_NEW_VIP_ESTIBAS01.xlsx     <- columnas DNI y APELLIDOS Y NOMBRES
│   └── Adjuntos\
│       ├── ANEXO_61256462_type2.pdf       <- el DNI va en el nombre del archivo
│       └── ...
├── ESTIBAS02\
├── CHOFERES DE KÍAS\
└── PACKING\                               <- cuando exista, se toma sola
```

El nombre de la carpeta define labor y tipo de personal. **Las reglas se evalúan en
este orden y gana la primera que coincide** — por eso `BUS` va antes que `CHOFER`:

| La carpeta contiene         | LABOR                 | TIPO DE PERSONAL |
|-----------------------------|-----------------------|------------------|
| `PACKING`                   | PACKING               | PROPIO           |
| `ESTIBA`                    | ESTIBADOR             | PROPIO           |
| `SEGURIDAD` o `PATRIMONIAL` | SEGURIDAD PATRIMONIAL | PROPIO           |
| `BUS`                       | CHOFER BUS            | TERCERO          |
| `CHOFER` o `KIA`            | CHOFER KIA            | TERCERO          |
| `CAMPO`                     | CAMPO                 | PROPIO           |

Una carpeta que no coincida con ninguna cae en `NO DEFINIDO / NO DEFINIDO`.
Agregar una cuadrilla nueva es crear la carpeta: el tablero la detecta sola.
Agregar una labor nueva es una línea en `LABOR_MAP` de `filtro/config.py`
(más su color en `COLOR_LABOR`, su etiqueta en `ui.LABOR_ESTILO` y su relleno
de Excel en `excel_export.FILL_LABOR`).

---

## Uso

**Elegir la carpeta** — en la barra lateral:

| Forma | Cómo |
|---|---|
| Explorador de Windows | Botón **📂 Examinar…** — abre el diálogo del sistema y se elige cualquier carpeta |
| Historial | Desplegable **Carpetas recientes** — las últimas 12 carpetas usadas, recordadas entre sesiones en `%USERPROFILE%\.filtro_antecedentes\carpetas_recientes.json` |
| A mano | Expander **Escribir la ruta a mano** — pegar la ruta y pulsar *Usar esta ruta* |

La app comprueba la carpeta antes de leerla y dice cuántas cuadrillas, Excel y PDF
encontró. Si se elige por error una carpeta de cuadrilla (o la carpeta de más arriba),
ofrece un botón para saltar a la carpeta correcta.

**Cabecera** — **total de personas** y **total de delitos** de la carpeta leída. Con
filtros activos ambos totales se recalculan sobre la vista filtrada.

**Barra lateral** — carpeta de trabajo, recarga, segmentación (labor, tipo de personal,
cuadrilla, veredicto, nivel, búsqueda) y descarga del Excel. Los filtros son globales:
afectan los KPI, todos los gráficos y el padrón a la vez.

**Las secciones no se pierden al filtrar.** La navegación superior guarda la sección
abierta en `session_state`, así que buscar un DNI o marcar una cuadrilla recalcula los
datos sin devolverte a *Resumen*.

**Secciones**

| Pestaña | Qué muestra |
|---|---|
| Planilla Prize | Cruce por DNI con el maestro: veredicto por área, cargos, situación en la empresa y quiénes no pertenecen a Prize |
| Resumen | KPI de decisión, personas por nivel (N1-N6 + **sin observaciones** + **sin verificar**), señales de alerta separadas por base (delitos / personas) y **todas** las tarjetas de acción inmediata |
| Propio vs tercero | Tasa de retiro, cobertura e índice medio por labor; composición por labor / tipo / cuadrilla |
| Matriz y categorías | Matriz de criticidad N1-N6 + **sin observaciones** + **sin verificar** + fila de total, delitos por categoría y la tabla completa de delitos en rojo |
| Vigencia | Delitos por año del proceso e índice de riesgo contra antigüedad |
| Padrón | Tabla expandible con el detalle por persona. **Ordenar por** (veredicto crítico → no crítico, nivel, índice, delitos, año, nombre, DNI, labor, cuadrilla) y **Sentido Desc ↓ / Asc ↑** para invertir el orden |
| Criterio N1-N6 | Las 21 reglas en el orden en que se evalúan y cómo se arma el índice |
| Trazabilidad | Qué se leyó, reglas de lectura y validación contra un archivo de referencia |

**Generar el Excel sin abrir el tablero**

```bat
python generar_excel.py
python generar_excel.py "D:\otra\ruta"
python generar_excel.py C:\FILTER --validar "C:\ruta\Resumen_NEW_VIP_Filtro_3.xlsx"
```

---

## Cómo se decide

**Solo cuenta el texto rojo.** En el reporte, los registros donde la persona figura como
denunciada, imputada o sentenciada van en rojo; donde figura como denunciante o agraviada
van en gris. Solo lo rojo se computa. Los reportes usan **dos rojos distintos**
(`#FC2727` y `#FF0000`) — detectar uno solo pierde registros reales.

**Se leen** las secciones DETALLE 2 en adelante: carpetas fiscales del Ministerio Público,
RENADESPPLE, INPE y detenciones policiales. DETALLE 1 son incidencias policiales sin
tipificación penal: se cuentan pero no aportan delitos.

**Clasificación N1-N6.** El texto del delito se normaliza y se evalúa contra 21 reglas
**en orden**; gana la primera que coincide. Por eso *LESIONES LEVES (AGRESIONES EN CONTRA
DE LAS MUJERES…)* cae en N5 familia y no en N6 lesiones. A cada persona se le asigna el
nivel más crítico de sus delitos. **N1-N3 → RETIRO. N4-N6 → SE ESTUDIA SALIDA.**

**Índice de riesgo 0-100** = gravedad (40 / 22 / 10) + vigencia (20 si ≥2023; 10 si
2018-2022; 2 si anterior) + reincidencia (0 / 7 / 13 / 20 según 1 / 2 / 3 / 4+ delitos)
+ caso activo (12) + dispersión geográfica (8).

**Veredicto** — CRITICO → NO APTO · ALTO → REVISION EN COMITE · MEDIO o BAJO → APTO CON
OBSERVACION · con reporte y sin delitos rojos → APTO · sin reporte → PENDIENTE DE REPORTE.

---

## Cambiar un criterio

Todo vive en **`filtro/config.py`**: la matriz, las 21 reglas, la gravedad por categoría,
los pesos del índice, el mapeo de carpetas y los colores. Edita ese archivo, pulsa
**Recargar** en el tablero y sube el archivo de referencia en la pestaña *Trazabilidad*
para confirmar que el cambio no rompió nada.

---

## Archivos

```
app.py                  tablero Streamlit
generar_excel.py        genera el Excel desde la línea de comandos
requirements.txt
run_windows.bat
.streamlit/config.toml  tema
filtro/
  config.py             TODOS los criterios y los dos temas — es el archivo que se edita
  maestro.py            maestro de funcionarios: JSON -> columnas, dedup y cruce por DNI
  carpetas.py           elección de carpeta: explorador nativo, historial, diagnóstico
                        y, en la web, subida de .zip / archivos sueltos
  pdf_reader.py         detección del rojo y lectura de los reportes
  classify.py           reglas N1-N6, gravedad, vigencia, índice
  loader.py             recorre la carpeta, cruza y arma los DataFrames
  excel_export.py       resultado_final.xlsx (7 hojas)
  charts.py             gráficos Altair
  ui.py                 paleta, CSS y componentes del tablero
  validate.py           comparación contra un archivo de referencia
```

## Lenguaje visual

El tablero usa el mismo código de color en todas partes, para que la decisión se lea
sin tener que interpretar números:

| Color | Significa | Dónde aparece |
|---|---|---|
| Rojo `#C0332E` | Retiro · N1-N3 · gravedad alta | fila del padrón, riel de la tarjeta, barras N1-N3, chip NO APTO |
| Ámbar `#E8A317` | Se estudia salida · N4 · revisión en comité | fila del padrón, barras N4, chip REVISION EN COMITE |
| Oliva `#7E8C33` | Se estudia salida · N5-N6 | barras N5-N6, chip de gravedad LEVE |
| Verde `#1F7A3D` | Sin observaciones | chip APTO |
| Gris `#8A8F8B` | Sin verificar — el punto ciego | chip PENDIENTE DE REPORTE |

En el padrón la fila entera se tiñe según el veredicto y lleva un riel del mismo color
a la izquierda; al hacer clic se despliega el detalle de sus delitos, con la gravedad y
el estado del caso también teñidos. Toda la paleta vive en `filtro/config.py` y
`filtro/ui.py`.

---

## Rendimiento

La primera lectura de 129 PDF toma unos 15 segundos. Después se cachean en
`<carpeta raíz>\.cache_filtro\pdfs.pkl` usando ruta + fecha + tamaño como llave, así que
las siguientes corridas son instantáneas y solo se releen los PDF que cambiaron.
*Limpiar caché* fuerza la relectura completa.

---

## Notas

- Un antecedente **no** equivale a una condena. Los estados «denuncia pendiente»,
  «en calificación» o «con acusación» son procesos en curso; la decisión final es del comité.
- Los delitos que la matriz no nombra (drogas, explosivos, usurpación, receptación, estafa,
  secuestro) se asignaron por analogía y quedan marcados en la columna `REGLA` de
  `Detalle_Delitos` para que el comité los reclasifique si corresponde.
- Información de uso restringido. El tablero corre local, no sube nada a internet.
