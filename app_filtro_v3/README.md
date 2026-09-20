# Filtro de Antecedentes · Prize / Aquanqa

Tablero Streamlit que apunta a **la carpeta que elija el usuario** (botón *Examinar…* en la
barra lateral; ya no hay ninguna ruta fija), cruza los DNI de
todos los `Resumen_NEW_VIP_*.xlsx` contra los reportes PDF de cada subcarpeta `Adjuntos`,
extrae **solo los delitos pintados en rojo**, los clasifica contra la matriz de criticidad
de Prize y genera `resultado_final.xlsx`.

Reproduce al 100% el archivo de referencia `Resumen_NEW_VIP_Filtro_3.xlsx`:
**30/30** valores de `FILTRO` y **360/360** celdas de `Resumen_Persona` idénticas.

---

## Instalación (Windows)

Doble clic en **`run_windows.bat`**. La primera vez crea el entorno virtual e instala
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
  config.py             TODOS los criterios — es el archivo que se edita
  carpetas.py           elección de carpeta: explorador nativo, historial y diagnóstico
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
