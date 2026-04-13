param(
    [string]$ProjectRoot = "C:\SignalDashboard"
)

$ErrorActionPreference = "Stop"

$PidPath = Join-Path $ProjectRoot "server.pid"

if (-not (Test-Path $PidPath)) {
    Write-Host "No PID file found: $PidPath"
    exit 0
}

$pidValue = (Get-Content $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
if (-not $pidValue) {
    Remove-Item $PidPath -Force -ErrorAction SilentlyContinue
    Write-Host "Empty PID file removed."
    exit 0
}

$proc = Get-Process -Id $pidValue -ErrorAction SilentlyContinue
if ($proc) {
    Stop-Process -Id $pidValue -Force
    Write-Host "Stopped Wizjoner PID $pidValue"
} else {
    Write-Host "Process PID $pidValue was not running."
}

Remove-Item $PidPath -Force -ErrorAction SilentlyContinue
