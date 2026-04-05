$ErrorActionPreference = "Stop"

$backendDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $backendDir
$webDir = Join-Path $projectRoot "frontend\\web"
$pythonExe = Join-Path $projectRoot ".venv\\Scripts\\python.exe"
$apiPort = 8000

function Stop-PortListeners {
  param([int]$TargetPort)
  $listeners = Get-NetTCPConnection -LocalPort $TargetPort -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique
  foreach ($procId in $listeners) {
    try { Stop-Process -Id $procId -Force -ErrorAction Stop; Write-Host "Stopped PID=$procId on port $TargetPort" }
    catch { Write-Host "Could not stop PID=$procId on port $TargetPort" }
  }
}

if (!(Test-Path $pythonExe)) { throw "Missing python executable: $pythonExe" }
if (!(Test-Path $webDir)) { throw "Missing frontend web directory: $webDir" }

Write-Host "Ensuring port $apiPort is free..."
Stop-PortListeners -TargetPort $apiPort

Write-Host "Building React web..."
Push-Location $webDir
npm install
npm run build
Pop-Location

Write-Host "Starting unified app at http://127.0.0.1:$apiPort"
& $pythonExe (Join-Path $backendDir "run_api.py")
