$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$webDir = Join-Path $root "web"
$pythonExe = Join-Path $root ".venv\Scripts\python.exe"

if (!(Test-Path $pythonExe)) {
  throw "Missing python executable: $pythonExe"
}

Write-Host "Building React web..."
Push-Location $webDir
npm install
npm run build
Pop-Location

Write-Host "Starting unified app at http://127.0.0.1:8000"
& $pythonExe (Join-Path $root "run_api.py")
