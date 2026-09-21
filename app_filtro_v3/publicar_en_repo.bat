@echo off
setlocal
chcp 65001 >nul
REM ============================================================
REM  Filtro de Antecedentes - Prize / Aquanqa
REM  Copia ESTA carpeta al repositorio de Git tal como debe
REM  quedar desplegada: todos los .py, sin cachés ni datos.
REM
REM  Uso:
REM     publicar_en_repo.bat "C:\ruta\al\repo\cienciadedatos_ia\app_filtro_v3"
REM
REM  Si no se indica ruta, la pide.
REM ============================================================
cd /d "%~dp0"

set "DESTINO=%~1"
if "%DESTINO%"=="" (
  echo.
  echo  Carpeta del repositorio donde vive la app desplegada.
  echo  Ejemplo: C:\Users\tu_usuario\repos\cienciadedatos_ia\app_filtro_v3
  echo.
  set /p DESTINO="Ruta destino: "
)
if "%DESTINO%"=="" (echo ERROR: no se indico destino. & pause & exit /b 1)

if not exist "%DESTINO%" (
  echo Creando "%DESTINO%"...
  mkdir "%DESTINO%" || (echo ERROR: no se pudo crear la carpeta. & pause & exit /b 1)
)

echo.
echo [1/3] Copiando el codigo...
robocopy "%CD%" "%DESTINO%" app.py generar_excel.py requirements.txt README.md run_windows.bat publicar_en_repo.bat .gitignore /NJH /NJS /NDL /NP >nul
robocopy "%CD%\filtro" "%DESTINO%\filtro" *.py /NJH /NJS /NDL /NP >nul
robocopy "%CD%\.streamlit" "%DESTINO%\.streamlit" config.toml /NJH /NJS /NDL /NP >nul
if errorlevel 8 (echo ERROR copiando archivos. & pause & exit /b 1)

echo [2/3] Quitando cache de Python del destino...
if exist "%DESTINO%\filtro\__pycache__" rmdir /s /q "%DESTINO%\filtro\__pycache__"
if exist "%DESTINO%\__pycache__" rmdir /s /q "%DESTINO%\__pycache__"
if exist "%DESTINO%\.cache_filtro" rmdir /s /q "%DESTINO%\.cache_filtro"

echo [3/3] Comprobando que esten los 9 archivos de codigo...
set FALTA=0
for %%F in (app.py generar_excel.py requirements.txt) do (
  if not exist "%DESTINO%\%%F" (echo   FALTA %%F & set FALTA=1)
)
for %%F in (__init__.py carpetas.py charts.py classify.py config.py excel_export.py loader.py maestro.py pdf_reader.py ui.py validate.py) do (
  if not exist "%DESTINO%\filtro\%%F" (echo   FALTA filtro\%%F & set FALTA=1)
)
if "%FALTA%"=="1" (echo. & echo Copia incompleta: revisa los avisos de arriba. & pause & exit /b 1)

echo.
echo  Listo. Codigo copiado en:
echo    %DESTINO%
echo.
echo  Ahora, desde la carpeta del repositorio:
echo.
echo    git rm -r --cached --ignore-unmatch "app_filtro_v3/filtro/__pycache__"
echo    git add -A
echo    git commit -m "Filtro de antecedentes v3.2: planilla, temas y selector de carpeta"
echo    git push
echo.
echo  Streamlit Cloud redespliega solo al recibir el push.
echo  Si la app muestra "Despliegue incompleto", es que algun archivo
echo  de filtro\ no llego al repositorio: repite estos pasos.
echo.
pause
