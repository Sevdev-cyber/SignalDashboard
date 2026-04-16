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
$RunnerScript = Join-Path $ProjectRoot "run_llm_worker_foreground.ps1"
$TaskName = "SignalDashboard-LlmWorker"

if (-not (Test-Path $ProjectRoot)) {
    throw "ProjectRoot not found: $ProjectRoot"
}

if (-not (Test-Path $WorkerScript)) {
    throw "llm_context_worker.py not found in: $ProjectRoot"
}

if (-not (Test-Path $RunnerScript)) {
    throw "run_llm_worker_foreground.ps1 not found in: $ProjectRoot"
}

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    $pythonCandidates = @(
        "C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe",
        "python.exe",
        "python"
    )
    foreach ($candidate in $pythonCandidates) {
        try {
            if ($candidate -like "*\*") {
                if (Test-Path $candidate) {
                    $PythonExe = $candidate
                    break
                }
            } else {
                $resolved = Get-Command $candidate -ErrorAction SilentlyContinue
                if ($resolved) {
                    $PythonExe = $resolved.Source
                    break
                }
            }
        } catch {
        }
    }
}

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    throw "Could not resolve python executable."
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

New-Item -ItemType Directory -Force -Path $ProjectRoot | Out-Null

$flagParts = @()
if ($Archive) { $flagParts += "-Archive" }
if ($DailyLlm) { $flagParts += "-DailyLlm" }
if ($IntradayLlm) { $flagParts += "-IntradayLlm" }
$runnerArgs = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $RunnerScript,
    "-ProjectRoot", $ProjectRoot,
    "-PythonExe", $PythonExe,
    "-PythonArgs", $PythonArgs,
    "-WsUrl", $WsUrl,
    "-IntervalSec", "$IntervalSec"
)
$runnerArgs += $flagParts
$runnerArgLine = ($runnerArgs | ForEach-Object {
    if ($_ -match '\s') { '"' + ($_ -replace '"', '\"') + '"' } else { $_ }
}) -join " "

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $runnerArgLine
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(5)
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName

$pidValue = ""
$waitDeadline = (Get-Date).AddSeconds(30)

while ((Get-Date) -lt $waitDeadline) {
    if (Test-Path $PidPath) {
        $pidRaw = Get-Content $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1
        $pidValue = if ($null -ne $pidRaw) { "$pidRaw".Trim() } else { "" }
        if ($pidValue -and (Get-Process -Id $pidValue -ErrorAction SilentlyContinue)) {
            break
        }
    }
    Start-Sleep -Milliseconds 750
}

if (-not $pidValue) {
    $taskInfo = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Get-ScheduledTaskInfo
    $taskState = if ($taskInfo) { "$($taskInfo.State) / LastResult=$($taskInfo.LastTaskResult)" } else { "unknown" }
    throw "LLM worker did not create PID file within timeout. Task state: $taskState. Check $ErrPath"
}

if (-not (Get-Process -Id $pidValue -ErrorAction SilentlyContinue)) {
    $taskInfo = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Get-ScheduledTaskInfo
    $taskState = if ($taskInfo) { "$($taskInfo.State) / LastResult=$($taskInfo.LastTaskResult)" } else { "unknown" }
    throw "LLM worker PID file exists but process is not running. Task state: $taskState. Check $ErrPath"
}

Write-Host "LLM worker headless started. PID=$pidValue"
Write-Host "Python executable: $PythonExe"
Write-Host "PID file: $PidPath"
Write-Host "Log file: $LogPath"
Write-Host "Error log: $ErrPath"
