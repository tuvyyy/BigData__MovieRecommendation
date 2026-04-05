$rootDir = Split-Path -Parent $MyInvocation.MyCommand.Path
& (Join-Path $rootDir 'start_web.ps1')
