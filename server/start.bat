@echo off
chcp 65001 >nul
cd /d "%~dp0"
title МебельМастер - сервер заявок

set PY=
where python >nul 2>nul && set PY=python
if not defined PY where py >nul 2>nul && set PY=py

if not defined PY (
  echo [X] Python не найден.
  echo     Скачай с python.org ^(3.10+^) и при установке поставь галочку "Add Python to PATH".
  pause
  exit /b 1
)

if not exist ".env" (
  if exist ".env.example" (
    copy ".env.example" ".env" >nul
    echo [!] Создал .env — впиши BOT_TOKEN от @BotFather, сохрани и запусти снова.
    notepad ".env"
    pause
    exit /b 0
  ) else (
    echo [X] Нет ни .env, ни .env.example
    pause
    exit /b 1
  )
)

echo [*] Проверка связи...
%PY% server.py --check
echo.
echo [*] Запускаю сервер...
%PY% server.py
pause