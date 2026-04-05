$root = Split-Path -Parent $PSScriptRoot
& (Join-Path $root 'status_web.ps1')
