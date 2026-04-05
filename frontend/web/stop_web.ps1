$root = Split-Path -Parent $PSScriptRoot
& (Join-Path $root 'stop_web.ps1')
