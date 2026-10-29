@echo off
REM Runs the service with only a JDK (17+) installed - no Maven needed.
REM Usage: run.bat [input.csv] [options]   e.g.  run.bat data\sample-transactions.csv --fail-before-commit TXN-2005:1
setlocal
cd /d "%~dp0"
if not exist target\classes mkdir target\classes
dir /s /b src\main\java\*.java > target\sources.txt
javac --release 17 -d target\classes @target\sources.txt || exit /b 1
java -cp target\classes com.fde.txnprocessor.App %*
