param(
    [string]$ProjectRoot = "C:\SignalDashboard",
    [int]$Tail = 80,
    [switch]$Follow
)

$ErrorActionPreference = "Stop"

$LogPath = Join-Path $ProjectRoot "server.log"

if (-not (Test-Path $LogPath)) {
    Write-Host "Log file not found: $LogPath"
    exit 0
}

if ($Follow) {
    Get-Content $LogPath -Tail $Tail -Wait
} else {
    Get-Content $LogPath -Tail $Tail
}
