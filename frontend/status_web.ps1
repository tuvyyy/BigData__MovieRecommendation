$port = 4173
$url = "http://127.0.0.1:$port"

$listeners = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
  Select-Object -ExpandProperty OwningProcess -Unique

if ($listeners) {
  Write-Host "Listening on port $port. PID(s): $($listeners -join ', ')"
}
else {
  Write-Host "No process listening on port $port."
}

try {
  $resp = Invoke-WebRequest -Uri "$url/@vite/client" -UseBasicParsing -TimeoutSec 3
  if ($resp.StatusCode -eq 200 -and $resp.Content -match 'vite/dist/client/env\.mjs') {
    Write-Host "Vite endpoint OK: $url"
  }
  else {
    Write-Host "Endpoint reachable but not Vite output."
  }
}
catch {
  Write-Host "HTTP check failed: $($_.Exception.Message)"
}
