$ErrorActionPreference = 'Stop'

$rootDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$webDir = Join-Path $rootDir 'web'
$logDir = Join-Path $rootDir 'logs'
$logPath = Join-Path $logDir 'web_dev.log'
$pidPath = Join-Path $logDir 'web_dev.pid'
$viteCacheDir = Join-Path $webDir 'node_modules\.vite'
$bindHost = '127.0.0.1'
$port = 4173
$baseUrl = "http://$bindHost`:$port"

function Stop-PortListeners {
  param([int]$TargetPort)
  $listeners = Get-NetTCPConnection -LocalPort $TargetPort -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique
  foreach ($procId in $listeners) {
    try {
      Stop-Process -Id $procId -Force -ErrorAction Stop
      Write-Host "Stopped PID=$procId on port $TargetPort"
    }
    catch {
      Write-Host "Could not stop PID=$procId on port $TargetPort"
    }
  }
}

function Test-ViteServer {
  param([string]$Url)
  try {
    $resp = Invoke-WebRequest -Uri "$Url/@vite/client" -UseBasicParsing -TimeoutSec 3
    return $resp.StatusCode -eq 200 -and $resp.Content -match 'vite/dist/client/env\.mjs'
  }
  catch {
    return $false
  }
}

function Test-AppEntryTransform {
  param([string]$Url)
  try {
    $resp = Invoke-WebRequest -Uri "$Url/src/main.tsx" -UseBasicParsing -TimeoutSec 3
    if ($resp.StatusCode -ne 200) {
      return $false
    }
    # This proves browser receives transformed JS from Vite, not raw TSX.
    return $resp.Content -match '__vite__cjsImport'
  }
  catch {
    return $false
  }
}

if (!(Test-Path $webDir)) {
  throw "Cannot find web directory: $webDir"
}
if (!(Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir | Out-Null
}
'' | Set-Content -Path $logPath -Encoding utf8

Stop-PortListeners -TargetPort $port

if (!(Test-Path (Join-Path $webDir 'node_modules'))) {
  Write-Host 'Installing frontend dependencies...'
  Push-Location $webDir
  npm install
  Pop-Location
}

if (Test-Path $viteCacheDir) {
  Remove-Item -Path $viteCacheDir -Recurse -Force -ErrorAction SilentlyContinue
}

$npmCmd = (Get-Command npm.cmd -ErrorAction Stop).Source

$cmd = "cd /d `"$webDir`" && `"$npmCmd`" run dev -- --host $bindHost --port $port --strictPort >> `"$logPath`" 2>&1"
$proc = Start-Process -FilePath cmd.exe -ArgumentList '/c', $cmd -PassThru
$proc.Id | Set-Content -Path $pidPath -Encoding ascii

$ready = $false
for ($i = 0; $i -lt 30; $i++) {
  Start-Sleep -Milliseconds 500
  if ((Test-ViteServer -Url $baseUrl) -and (Test-AppEntryTransform -Url $baseUrl)) {
    $ready = $true
    break
  }
}

if ($ready) {
  Write-Host "Web started successfully: $baseUrl (PID=$($proc.Id))"
}
else {
  Write-Host "Web process started (PID=$($proc.Id)) but Vite endpoint not ready yet."
  Write-Host 'Check logs for details.'
}

Write-Host "Log file: $logPath"
