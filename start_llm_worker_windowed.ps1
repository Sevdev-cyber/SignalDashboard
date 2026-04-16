param(
    [string]$ProjectRoot = "C:\SignalDashboard",
    [string]$PythonExe = "",
    [string]$PythonArgs = "-u",
    [string]$WsUrl = "wss://web-production-3ff3f.up.railway.app/ws",
    [int]$IntervalSec = 10,
    [switch]$Archive = $true,
    [switch]$DailyLlm = $true,
    [switch]$IntradayLlm = $true
)

$ErrorActionPreference = "Stop"

$PidPath = Join-Path $ProjectRoot "llm_worker.pid"
$RunnerScript = Join-Path $ProjectRoot "run_llm_worker_foreground.ps1"

if (-not (Test-Path $ProjectRoot)) {
    throw "ProjectRoot not found: $ProjectRoot"
}

if (-not (Test-Path $RunnerScript)) {
    throw "run_llm_worker_foreground.ps1 not found in: $ProjectRoot"
}

if (Test-Path $PidPath) {
    $oldPid = (Get-Content $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
    if ($oldPid) {
        $existing = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
        if ($existing) {
            Write-Host "LLM worker already running with PID $oldPid"
            exit 0
        }
    }
}

$argList = @(
    "-NoLogo",
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $RunnerScript,
    "-ProjectRoot", $ProjectRoot,
    "-WsUrl", $WsUrl,
    "-IntervalSec", "$IntervalSec"
)

if (-not [string]::IsNullOrWhiteSpace($PythonExe)) {
    $argList += @("-PythonExe", $PythonExe)
}
if (-not [string]::IsNullOrWhiteSpace($PythonArgs)) {
    $argList += @("-PythonArgs", $PythonArgs)
}
if ($Archive) { $argList += "-Archive" }
if ($DailyLlm) { $argList += "-DailyLlm" }
if ($IntradayLlm) { $argList += "-IntradayLlm" }

$proc = Start-Process -FilePath "powershell.exe" -ArgumentList $argList -WorkingDirectory $ProjectRoot -PassThru

$pidValue = ""
$waitDeadline = (Get-Date).AddSeconds(15)
while ((Get-Date) -lt $waitDeadline) {
    if (Test-Path $PidPath) {
        $pidRaw = Get-Content $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1
        $pidValue = if ($null -ne $pidRaw) { "$pidRaw".Trim() } else { "" }
        if ($pidValue) {
            break
        }
    }
    Start-Sleep -Milliseconds 500
}

if (-not $pidValue) {
    Write-Host "Worker window opened but PID file is not ready yet."
    Write-Host "Window PID: $($proc.Id)"
    exit 0
}

Write-Host "LLM worker window started. PID=$pidValue"
Write-Host "Launcher window PID: $($proc.Id)"
Write-Host "PID file: $PidPath"
