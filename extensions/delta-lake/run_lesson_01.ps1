param(
    [Parameter(Mandatory = $true)][string]$PythonExecutable,
    [Parameter(Mandatory = $true)][string]$RuntimeDirectory
)

$ErrorActionPreference = 'Stop'
$previousRuntime = $env:KREAM_DELTA_RUNTIME_DIR
$previousEncoding = $env:PYTHONIOENCODING
try {
    $env:KREAM_DELTA_RUNTIME_DIR = $RuntimeDirectory
    $env:PYTHONIOENCODING = 'utf-8'
    & $PythonExecutable (Join-Path $PSScriptRoot 'lessons/01_delta_versions.py')
    if ($LASTEXITCODE -ne 0) { throw "Delta lesson failed: exit code $LASTEXITCODE" }
}
finally {
    $env:KREAM_DELTA_RUNTIME_DIR = $previousRuntime
    $env:PYTHONIOENCODING = $previousEncoding
}
