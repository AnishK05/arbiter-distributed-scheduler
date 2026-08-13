#Requires -Version 5.1
<#
.SYNOPSIS
  PowerShell entrypoint for Arbiter — Windows-native alternative to Make.

.DESCRIPTION
  Mirrors the common Makefile targets so you can run the full demo from
  Windows PowerShell or PowerShell 7 without WSL.

  Prerequisites: Docker Desktop (WSL2 backend), Go 1.22+, Python 3, git.

.EXAMPLE
  .\scripts\arbiter.ps1 demo-up
  .\scripts\arbiter.ps1 build
  .\scripts\arbiter.ps1 demo-verify
  .\scripts\arbiter.ps1 demo-down

.EXAMPLE
  .\scripts\arbiter.ps1 up
  .\scripts\arbiter.ps1 test
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet(
        'help', 'build', 'clean', 'vet', 'test', 'tidy', 'fmt',
        'up', 'down',
        'phase4-up', 'phase4-down',
        'phase6-up', 'phase6-down',
        'phase7-up', 'phase7-down',
        'phase8-up', 'phase8-down',
        'phase9-up', 'phase9-down',
        'demo-up', 'demo-down', 'demo-verify'
    )]
    [string]$Command = 'help'
)

$ErrorActionPreference = 'Stop'

function Get-RepoRoot {
    return (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}

function Assert-CommandExists([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found on PATH: $Name"
    }
}

function Get-Python {
    foreach ($candidate in @('python', 'py', 'python3')) {
        $cmd = Get-Command $candidate -ErrorAction SilentlyContinue
        if (-not $cmd) { continue }
        if ($candidate -eq 'py') {
            return @{ Exe = $cmd.Source; PrefixArgs = @('-3') }
        }
        # Prefer real Python; skip the Windows Store stub when possible.
        try {
            $ver = & $cmd.Source --version 2>&1 | Out-String
            if ($ver -match 'Python 3') {
                return @{ Exe = $cmd.Source; PrefixArgs = @() }
            }
        } catch {
            continue
        }
    }
    throw "Python 3 not found. Install from https://www.python.org/downloads/ and ensure 'python' is on PATH."
}

function Get-GoLdflags {
    $module = 'github.com/AnishK05/arbiter-distributed-scheduler'
    $version = 'dev'
    $commit = 'none'
    try { $version = (git describe --tags --always --dirty 2>$null).Trim() } catch { }
    if (-not $version) { $version = 'dev' }
    try { $commit = (git rev-parse --short HEAD 2>$null).Trim() } catch { }
    if (-not $commit) { $commit = 'none' }
    return "-X '${module}/internal/buildinfo.Version=${version}' -X '${module}/internal/buildinfo.Commit=${commit}'"
}

function Invoke-DockerCompose {
    param(
        [string[]]$ComposeFiles,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$ComposeArgs
    )
    Assert-CommandExists 'docker'
    $argv = @('compose')
    foreach ($f in $ComposeFiles) {
        $argv += @('-f', $f)
    }
    $argv += $ComposeArgs
    Write-Host ">> docker $($argv -join ' ')"
    & docker @argv
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Build {
    Assert-CommandExists 'go'
    $root = Get-RepoRoot
    $bin = Join-Path $root 'bin'
    New-Item -ItemType Directory -Force -Path $bin | Out-Null
    $ldflags = Get-GoLdflags
    $targets = @(
        @{ Out = 'scheduler.exe'; Pkg = './cmd/scheduler' },
        @{ Out = 'worker.exe';    Pkg = './cmd/worker' },
        @{ Out = 'arbiterctl.exe'; Pkg = './cmd/arbiterctl' }
    )
    Push-Location $root
    try {
        foreach ($t in $targets) {
            $out = Join-Path $bin $t.Out
            Write-Host ">> go build -o $($t.Out) $($t.Pkg)"
            & go build -ldflags $ldflags -o $out $t.Pkg
            if ($LASTEXITCODE -ne 0) { throw "go build failed for $($t.Pkg)" }
        }
        # Convenience copies without .exe for scripts that default to ./bin/arbiterctl
        Copy-Item -Force (Join-Path $bin 'arbiterctl.exe') (Join-Path $bin 'arbiterctl') -ErrorAction SilentlyContinue
    } finally {
        Pop-Location
    }
    Write-Host "Built binaries in $bin"
}

function Invoke-DemoVerify {
    $verify = Join-Path $PSScriptRoot 'verify_demo.ps1'
    & $verify
    if ($LASTEXITCODE -ne 0) { throw "demo-verify failed" }
}

function Remove-AutoscaledWorkers {
    Assert-CommandExists 'docker'
    $ids = @(docker ps -aq --filter 'label=arbiter.autoscaled=true' 2>$null)
    if ($ids -and $ids.Count -gt 0 -and $ids[0]) {
        Write-Host "Removing autoscaled workers: $($ids -join ', ')"
        & docker rm -f @ids | Out-Null
    }
}

function Show-Help {
    @"
Arbiter PowerShell helper (Windows-native Make alternative)

Usage:
  .\scripts\arbiter.ps1 <command>

Commands:
  demo-up       Start full demo (3 schedulers, 10 workers, observability, dashboard)
  demo-down     Stop demo cluster (+ autoscaled leftovers)
  demo-verify   Smoke-check healthz, 10 ready nodes, sample submit
  build         Build scheduler/worker/arbiterctl into .\bin\*.exe
  clean         Remove .\bin
  up / down     Minimal 1-scheduler + 1-worker stack
  phase4-up|down … phase9-up|down
  vet / test / tidy / fmt

URLs after demo-up:
  Dashboard   http://localhost:3100
  Grafana     http://localhost:3000  (admin/admin)
  Prometheus  http://localhost:9090
  API / gRPC  http://localhost:8080  /  localhost:7000

Full guide: docs\local-setup.md
"@ | Write-Host
}

$root = Get-RepoRoot
Set-Location $root

$demo = @('deploy/docker-compose.demo.yml')
$base = @('deploy/docker-compose.yml')
$p4 = $base + @('deploy/docker-compose.phase4.yml')
$p6 = $base + @('deploy/docker-compose.phase6.yml')
$p7 = $p6 + @('deploy/docker-compose.phase7.yml')
$p8 = $p7 + @('deploy/docker-compose.phase8.yml')
$p9 = $p8 + @('deploy/docker-compose.phase9.yml')

switch ($Command) {
    'help' { Show-Help }
    'build' { Invoke-Build }
    'clean' {
        $bin = Join-Path $root 'bin'
        if (Test-Path $bin) { Remove-Item -Recurse -Force $bin }
        Write-Host "Removed $bin"
    }
    'vet' {
        Assert-CommandExists 'go'
        & go vet ./...
        if ($LASTEXITCODE -ne 0) { throw 'go vet failed' }
    }
    'test' {
        Assert-CommandExists 'go'
        & go test ./... -race -count=1
        if ($LASTEXITCODE -ne 0) { throw 'go test failed' }
    }
    'tidy' {
        Assert-CommandExists 'go'
        & go mod tidy
        if ($LASTEXITCODE -ne 0) { throw 'go mod tidy failed' }
    }
    'fmt' {
        Assert-CommandExists 'go'
        & gofmt -l -w .
    }
    'up' { Invoke-DockerCompose -ComposeFiles $base -ComposeArgs @('up', '-d', '--build') }
    'down' { Invoke-DockerCompose -ComposeFiles $base -ComposeArgs @('down') }
    'phase4-up' { Invoke-DockerCompose -ComposeFiles $p4 -ComposeArgs @('up', '-d', '--build') }
    'phase4-down' { Invoke-DockerCompose -ComposeFiles $p4 -ComposeArgs @('down') }
    'phase6-up' { Invoke-DockerCompose -ComposeFiles $p6 -ComposeArgs @('up', '-d', '--build') }
    'phase6-down' { Invoke-DockerCompose -ComposeFiles $p6 -ComposeArgs @('down') }
    'phase7-up' { Invoke-DockerCompose -ComposeFiles $p7 -ComposeArgs @('up', '-d', '--build') }
    'phase7-down' { Invoke-DockerCompose -ComposeFiles $p7 -ComposeArgs @('down') }
    'phase8-up' { Invoke-DockerCompose -ComposeFiles $p8 -ComposeArgs @('up', '-d', '--build') }
    'phase8-down' { Invoke-DockerCompose -ComposeFiles $p8 -ComposeArgs @('down') }
    'phase9-up' { Invoke-DockerCompose -ComposeFiles $p9 -ComposeArgs @('up', '-d', '--build') }
    'phase9-down' { Invoke-DockerCompose -ComposeFiles $p9 -ComposeArgs @('down') }
    'demo-up' { Invoke-DockerCompose -ComposeFiles $demo -ComposeArgs @('up', '-d', '--build') }
    'demo-down' {
        Invoke-DockerCompose -ComposeFiles $demo -ComposeArgs @('down', '--remove-orphans')
        Remove-AutoscaledWorkers
    }
    'demo-verify' { Invoke-DemoVerify }
    default { Show-Help; throw "Unknown command: $Command" }
}
