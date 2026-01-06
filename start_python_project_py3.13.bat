@echo off
rem === カレントをこの BAT があるフォルダへ移動 ===
cd /d "%~dp0"

rem === 既存の .venv_* フォルダを探す ===
set "VENV_DIR="
for /d %%d in (.venv_*) do (
    set "VENV_DIR=%%d"
    goto :VENV_FOUND
)

rem === なければユーザーに任意名を入力してもらう ===
echo 仮想環境フォルダが見つかりませんでしたわ。
set /p VENV_SUFFIX=作成する仮想環境名を入力してください（例: alpha）: 
if "%VENV_SUFFIX%"=="" (
    echo 名称が空では作成できませんわ。終了いたします。
    pause
    exit /b 1
)
set "VENV_DIR=.venv_%VENV_SUFFIX%"

echo 仮想環境 %VENV_DIR% を作成いたしますわ…
"C:\Python\Python313\python.exe" -m venv "%VENV_DIR%"
if %ERRORLEVEL% neq 0 (
    echo 仮想環境の作成に失敗いたしましたわ。Python のパスをご確認くださいませ。
    pause
    exit /b 1
)

:VENV_FOUND
echo 使用する仮想環境: %VENV_DIR%

rem === requirements.txt があれば、その仮想環境の pip でインストール ===
if exist "requirements.txt" (
    echo パッケージをインストールいたしますわ…
    "%VENV_DIR%\Scripts\python.exe" -m pip install --upgrade pip
    "%VENV_DIR%\Scripts\python.exe" -m pip install -r requirements.txt
    if %ERRORLEVEL% neq 0 (
        echo パッケージのインストールに失敗いたしましたわ。
        pause
        exit /b 1
    )
) else (
    echo requirements.txt が見つかりませんでしたわ。スキップいたします。
)

rem === VSCode のワークスペース設定を書き出す ===
if not exist ".vscode" mkdir ".vscode"

(
    echo {
    echo     "python.defaultInterpreterPath": "%~dp0%VENV_DIR%\Scripts\python.exe"
    echo }
) > ".vscode\settings.json"


rem === VSCode を起動 ===
echo VSCode を現在のディレクトリで起動いたしますわ…
code .

pause
