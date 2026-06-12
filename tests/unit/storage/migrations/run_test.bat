@echo off
cd /d "%~dp0..\..\..\..\"

echo =======================================================
echo     QUY TRINH KIEM THU TU DONG HOA (CAY THU MUC TEST)
echo =======================================================
echo Vi tri dung hien tai: %CD%

echo 1. Don dep container cu...
docker rm -f camara-db-test >nul 2>&1

echo 2. Khoi chay PostgreSQL Container...
docker run --name camara-db-test -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=camara_network -p 5432:5432 -d postgres:16-alpine

echo 3. Cho 3 giay de Postgres san sang...
timeout /t 3 /nobreak >nul

echo 4. Kich hoat Pytest...
:: Ép cứng các biến môi trường cho phiên chạy này để đè lên biến cũ của máy
set DB_HOST=127.0.0.1
set DB_PORT=5432
set DB_USER=postgres
set DB_PASSWORD=postgres
set DB_NAME=camara_network

pytest tests/unit/storage/migrations -v
set TEST_RESULT=%ERRORLEVEL%

echo 5. Tu dong don dep va xoa container...
docker rm -f camara-db-test >nul 2>&1

echo =======================================================
if %TEST_RESULT% EQU 0 (
    echo PHAN TEST: PASSED HOAN HAO!
) else (
    echo PHAN TEST: FAILED!
)
echo =======================================================

exit /b %TEST_RESULT%