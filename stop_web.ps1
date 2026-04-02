$ErrorActionPreference = 'SilentlyContinue'

$rootDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$logDir = Join-Path $rootDir 'logs'
$pidPath = Join-Path $logDir 'web_dev.pid'
$ports = @(4173, 5173)

if (Test-Path $pidPath) {
  $pidValue = Get-Content -Path $pidPath -ErrorAction SilentlyContinue
  if ($pidValue) {
    Stop-Process -Id ([int]$pidValue) -Force
    Write-Host "Stopped web process from PID file: $pidValue"
  }
  Remove-Item $pidPath -Force -ErrorAction SilentlyContinue
}

foreach ($port in $ports) {
  $listenerPids = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique

  if (!$listenerPids) {
    Write-Host "No process is listening on $port."
    continue
  }

  foreach ($procId in $listenerPids) {
    Stop-Process -Id $procId -Force
    Write-Host "Stopped PID=$procId on port $port"
  }
}
