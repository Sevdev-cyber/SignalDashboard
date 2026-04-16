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

$LogPath = Join-Path $ProjectRoot "llm_worker.log"
$ErrPath = Join-Path $ProjectRoot "llm_worker.err.log"
$PidPath = Join-Path $ProjectRoot "llm_worker.pid"
$WorkerScript = Join-Path $ProjectRoot "llm_context_worker.py"

function Resolve-PythonExe([string]$RequestedExe) {
    if (-not [string]::IsNullOrWhiteSpace($RequestedExe)) {
        return $RequestedExe
    }

    $pythonCandidates = @(
        "C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe",
        "python.exe",
        "python"
    )

    foreach ($candidate in $pythonCandidates) {
        try {
            if ($candidate -like "*\*") {
                if (Test-Path $candidate) {
                    return $candidate
                }
            } else {
                $resolved = Get-Command $candidate -ErrorAction SilentlyContinue
                if ($resolved) {
                    return $resolved.Source
                }
            }
        } catch {
        }
    }

    throw "Could not resolve python executable."
}

if (-not (Test-Path $ProjectRoot)) {
    throw "ProjectRoot not found: $ProjectRoot"
}

if (-not (Test-Path $WorkerScript)) {
    throw "llm_context_worker.py not found in: $ProjectRoot"
}

$PythonExe = Resolve-PythonExe $PythonExe

New-Item -ItemType Directory -Force -Path $ProjectRoot | Out-Null
Set-Content -Path $PidPath -Value $PID

$pythonArgList = @()
if ($PythonArgs) {
    $pythonArgList += ($PythonArgs -split ' ' | Where-Object { $_ -and $_.Trim() })
}
$pythonArgList += @(
    $WorkerScript,
    "--ws-url", $WsUrl,
    "--interval", "$IntervalSec"
)
if ($Archive) { $pythonArgList += "--archive" }
if ($DailyLlm) { $pythonArgList += "--daily-llm" }
if ($IntradayLlm) { $pythonArgList += "--intraday-llm" }

try {
    Set-Location $ProjectRoot
    & $PythonExe $pythonArgList 1>> $LogPath 2>> $ErrPath
} finally {
    if (Test-Path $PidPath) {
        $pidRaw = Get-Content $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($null -ne $pidRaw -and "$pidRaw".Trim() -eq "$PID") {
            Remove-Item $PidPath -Force -ErrorAction SilentlyContinue
        }
    }
}
