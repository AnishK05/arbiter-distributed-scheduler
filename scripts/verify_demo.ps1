#Requires -Version 5.1
<#
.SYNOPSIS
  Smoke-check the Phase 10 demo stack after demo-up (PowerShell).

.EXAMPLE
  .\scripts\arbiter.ps1 demo-verify
  .\scripts\verify_demo.ps1
#>
[CmdletBinding()]
param(
    [string]$ApiBase = $(if ($env:ARBITER_HTTP_ADDR) { $env:ARBITER_HTTP_ADDR } else { 'http://localhost:8080' }),
    [string]$SchedulerAddr = $(if ($env:ARBITER_SCHEDULER_ADDR) { $env:ARBITER_SCHEDULER_ADDR } else { 'localhost:7000' }),
    [int]$MinReady = 10
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $root

function Get-Python {
    foreach ($candidate in @('python', 'py', 'python3')) {
        $cmd = Get-Command $candidate -ErrorAction SilentlyContinue
        if (-not $cmd) { continue }
        if ($candidate -eq 'py') {
            return @{ Exe = $cmd.Source; PrefixArgs = @('-3') }
        }
        try {
            $ver = & $cmd.Source --version 2>&1 | Out-String
            if ($ver -match 'Python 3') {
                return @{ Exe = $cmd.Source; PrefixArgs = @() }
            }
        } catch { continue }
    }
    throw "Python 3 not found on PATH"
}

function Resolve-Arbiterctl {
    $candidates = @(
        (Join-Path $root 'bin\arbiterctl.exe'),
        (Join-Path $root 'bin\arbiterctl'),
        '.\bin\arbiterctl.exe',
        '.\bin\arbiterctl'
    )
    foreach ($c in $candidates) {
        if (Test-Path $c) { return (Resolve-Path $c).Path }
    }
    return $null
}

Write-Host '== healthz =='
$health = Invoke-WebRequest -Uri "$ApiBase/healthz" -UseBasicParsing
Write-Host $health.Content.Trim()

Write-Host "== nodes (expect $MinReady ready) =="
$nodesJson = (Invoke-WebRequest -Uri "$ApiBase/api/v1/nodes" -UseBasicParsing).Content
$py = Get-Python
$countScript = @'
import json, sys
ns = json.load(sys.stdin).get("nodes") or []
ready = sum(1 for n in ns if n.get("status") == "ready")
print(ready)
print(f"ready={ready} total={len(ns)}", file=sys.stderr)
'@
$readyStr = $nodesJson | & $py.Exe @($py.PrefixArgs + @('-c', $countScript))
$ready = [int]("$readyStr".Trim())
if ($ready -lt $MinReady) {
    throw "FAIL: only $ready ready nodes (want >= $MinReady)"
}

$arbiterctl = Resolve-Arbiterctl
if (-not $arbiterctl) {
    Write-Warning "arbiterctl missing — run .\scripts\arbiter.ps1 build to exercise CLI submit"
    Write-Host "ok: demo API healthy with $ready ready workers"
    exit 0
}

$jobName = "verify-demo-$PID"
Write-Host "== submit $jobName (3 replicas) =="
& $arbiterctl --scheduler-addr $SchedulerAddr submit $jobName --replicas 3 --wait --wait-timeout 90s
if ($LASTEXITCODE -ne 0) {
    throw "arbiterctl submit failed with exit code $LASTEXITCODE"
}

Write-Host "ok: demo verified ($ready ready workers + job succeeded)"
