@echo off
REM ============================================================
REM  Filtro de Antecedentes - Prize / Aquanqa
REM  Doble clic para levantar el tablero.
REM ============================================================
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [1/3] Creando entorno virtual...
  python -m venv .venv || (echo ERROR: no se encontro Python. Instalalo desde python.org y marca "Add to PATH". & pause & exit /b 1)
  echo [2/3] Instalando dependencias...
  ".venv\Scripts\python.exe" -m pip install --upgrade pip -q
  ".venv\Scripts\python.exe" -m pip install -r requirements.txt || (echo ERROR instalando dependencias. & pause & exit /b 1)
)

echo [3/3] Levantando el tablero...
".venv\Scripts\python.exe" -m streamlit run app.py
pause
