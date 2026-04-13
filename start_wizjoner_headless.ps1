param(
    [string]$ProjectRoot = "C:\SignalDashboard",
    [string]$PythonExe = "python",
    [string]$Host = "127.0.0.1",
    [int]$TcpPort = 5557,
    [int]$WsPort = 8082,
    [string]$RelayUrl = "https://web-production-3ff3f.up.railway.app/push",
    [string]$RelaySecret = "SacredForestSignal123",
    [string]$AccountName = "Playback101",
    [string]$BarTfMin = "1",
    [string]$EngineMode = "final_mtf_v3"
)

$ErrorActionPreference = "Stop"

$LogPath = Join-Path $ProjectRoot "server.log"
$PidPath = Join-Path $ProjectRoot "server.pid"
$ServerScript = Join-Path $ProjectRoot "signal_server.py"

if (-not (Test-Path $ProjectRoot)) {
    throw "ProjectRoot not found: $ProjectRoot"
}

if (-not (Test-Path $ServerScript)) {
    throw "signal_server.py not found in: $ProjectRoot"
}

if (Test-Path $PidPath) {
    $oldPid = (Get-Content $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
    if ($oldPid) {
        $existing = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
        if ($existing) {
            Write-Host "Wizjoner already running with PID $oldPid"
            exit 0
        }
    }
}

New-Item -ItemType Directory -Force -Path $ProjectRoot | Out-Null

$cmd = @(
    "/c",
    "cd /d $ProjectRoot",
    "&& set DASHBOARD_BAR_TF_MIN=$BarTfMin",
    "&& set SIGNAL_ENGINE_MODE=$EngineMode",
    "&& set NT_ACCOUNT_NAME=$AccountName",
    "&& $PythonExe $ServerScript --host $Host --port $TcpPort --ws-port $WsPort --relay-url $RelayUrl --relay-secret $RelaySecret --account $AccountName >> `"$LogPath`" 2>&1"
) -join " "

$proc = Start-Process -FilePath "cmd.exe" `
    -ArgumentList $cmd `
    -WorkingDirectory $ProjectRoot `
    -WindowStyle Hidden `
    -PassThru

Set-Content -Path $PidPath -Value $proc.Id

Write-Host "Wizjoner headless started. PID=$($proc.Id)"
Write-Host "PID file: $PidPath"
Write-Host "Log file: $LogPath"
