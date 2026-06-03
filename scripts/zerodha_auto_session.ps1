param(
    [string]$Ec2Host = $(if ($env:QUANT_AWS_HOST) { $env:QUANT_AWS_HOST } else { "13.51.193.234" }),
    [string]$Ec2User = $(if ($env:QUANT_AWS_USER) { $env:QUANT_AWS_USER } else { "ec2-user" }),
    [string]$KeyPath = $(if ($env:QUANT_AWS_KEY) { $env:QUANT_AWS_KEY } else { "C:\Users\Shivam\.ssh\aham-new.pem" }),
    [string]$RemoteProjectDir = $(if ($env:QUANT_AWS_PROJECT_DIR) { $env:QUANT_AWS_PROJECT_DIR } else { "/home/ec2-user/newQuant/Quant" }),
    [string]$EnvFile = ".env",
    [int]$CallbackPort = 8765,
    [string]$CallbackPath = "/zerodha/callback",
    [int]$TimeoutSeconds = 180,
    [string]$RequestToken = "",
    [switch]$Help
)

$ErrorActionPreference = "Stop"

if ($Help) {
    Write-Output @"
Usage:
  powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\zerodha_auto_session.ps1

This opens Kite login, captures request_token at http://127.0.0.1:8765/zerodha/callback,
generates ZERODHA_ACCESS_TOKEN, pushes it to AWS, restarts quant.service, and runs preflight.

If the Kite app redirect URL is not configured to the local callback, pass the request token:
  powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\zerodha_auto_session.ps1 -RequestToken "<request_token>"
"@
    exit 0
}

function Get-EnvValue {
    param([string]$Path, [string]$Key)
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line.StartsWith("$Key=")) {
            return $line.Substring($Key.Length + 1).Trim()
        }
    }
    return ""
}

if (-not (Test-Path -LiteralPath $EnvFile)) {
    throw "Env file not found: $EnvFile"
}

$argsList = @(
    "-m", "bot.cli", "zerodha-auto-session",
    "--write-env",
    "--env-file", $EnvFile,
    "--callback-port", "$CallbackPort",
    "--callback-path", $CallbackPath,
    "--timeout-seconds", "$TimeoutSeconds",
    "--open-browser"
)
if ($RequestToken) {
    $argsList += @("--request-token", $RequestToken)
}

python @argsList
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$accessToken = Get-EnvValue -Path $EnvFile -Key "ZERODHA_ACCESS_TOKEN"
if (-not $accessToken) {
    throw "ZERODHA_ACCESS_TOKEN was not written to $EnvFile"
}

$remote = "$Ec2User@$Ec2Host"
$sshBase = @("-i", $KeyPath, "-o", "StrictHostKeyChecking=no", $remote)

$accessToken | ssh @sshBase "cd '$RemoteProjectDir' && scripts/zerodha_set_access_token.sh -"
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

ssh @sshBase "podman exec quant python -m bot.cli zerodha-preflight --asset crudeoil --asset naturalgas --for-live --online"
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Output "ZERODHA_AUTO_SESSION_OK local_env=$EnvFile remote=$remote project=$RemoteProjectDir"
