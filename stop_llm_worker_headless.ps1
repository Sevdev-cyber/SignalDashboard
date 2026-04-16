param(
    [string]$ProjectRoot = "C:\SignalDashboard"
)

$ErrorActionPreference = "Stop"

$PidPath = Join-Path $ProjectRoot "llm_worker.pid"
$TaskName = "SignalDashboard-LlmWorker"
$WorkerScript = Join-Path $ProjectRoot "llm_context_worker.py"

try {
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null
} catch {
}

if (-not (Test-Path $PidPath)) {
    Write-Host "No PID file found: $PidPath"
    exit 0
}

$pidRaw = Get-Content $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1
$pidValue = if ($null -ne $pidRaw) { "$pidRaw".Trim() } else { "" }
if ([string]::IsNullOrWhiteSpace($pidValue)) {
    Remove-Item $PidPath -Force -ErrorAction SilentlyContinue
    Write-Host "Empty PID file removed."
    exit 0
}

$proc = Get-Process -Id $pidValue -ErrorAction SilentlyContinue
if ($proc) {
    Stop-Process -Id $pidValue -Force
    Write-Host "Stopped LLM worker PID $pidValue"
} else {
    Write-Host "Process PID $pidValue was not running."
}

$workerMatches = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -and $_.CommandLine.Contains($WorkerScript)
})

foreach ($match in $workerMatches) {
    try {
        Stop-Process -Id $match.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "Stopped worker child PID $($match.ProcessId)"
    } catch {
    }
}

for ($i = 0; $i -lt 10; $i++) {
    Remove-Item $PidPath -Force -ErrorAction SilentlyContinue
    if (-not (Test-Path $PidPath)) {
        Write-Host "Removed PID file."
        exit 0
    }
    Start-Sleep -Milliseconds 300
}

Write-Host "PID file still present after stop: $PidPath"
