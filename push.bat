@echo off
:: Cambiar codificacion a UTF-8
chcp 65001 > nul

:: Moverse automaticamente a la carpeta donde esta guardado este script
cd /d "%~dp0"

echo =======================================
echo Actualizando repositorio: AUTOMATIZACION
echo =======================================
echo.

:: 1. Crear .gitignore si no existe
if not exist .gitignore (
    echo [*] Creando archivo .gitignore de seguridad...
    (
        echo # Archivos temporales de Python
        echo __pycache__/
        echo *.pyc
        echo.
        echo # Claves y configuracion local
        echo .env
        echo local_test.py
        echo local_test_mock.py
    ) > .gitignore
)

:: 2. Inicializar Git si no existe
if not exist .git (
    echo [*] Inicializando repositorio Git local...
    git init
    git branch -M main
    echo.
    echo --------------------------------------------------------
    echo NOTA: Si es la primera vez, necesitaras asociar el remoto.
    echo Puedes hacerlo ejecutando en tu terminal:
    echo git remote add origin https://github.com/diegoriverodlh/cloud.git
    echo --------------------------------------------------------
    echo.
)

:: 3. Proceso de Git
echo [*] Anadiendo cambios...
git add .

echo.
set /p mensaje="Mensaje del commit: "

echo.
echo [*] Generando commit...
git commit -m "%mensaje%"

echo.
echo [*] Subiendo cambios a GitHub...
git push origin main

echo.
echo =======================================
echo Push completado con exito.
echo =======================================
pause