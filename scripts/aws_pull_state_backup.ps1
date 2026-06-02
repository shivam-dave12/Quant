param(
    [string]$Ec2Host = $env:QUANT_AWS_HOST,
    [string]$Ec2User = $(if ($env:QUANT_AWS_USER) { $env:QUANT_AWS_USER } else { "ec2-user" }),
    [string]$KeyPath = $env:QUANT_AWS_KEY,
    [string]$RemoteStateDir = $(if ($env:QUANT_AWS_STATE_DIR) { $env:QUANT_AWS_STATE_DIR } else { "/home/ec2-user/quant/state" }),
    [string]$LocalBackupRoot = $(if ($env:QUANT_LOCAL_BACKUP_ROOT) { $env:QUANT_LOCAL_BACKUP_ROOT } else { (Join-Path $PSScriptRoot "..\aws_state_backups") }),
    [int]$IntervalMinutes = 0,
    [int]$RetentionDays = 14,
    [switch]$Once
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-SshArgs {
    $args = @("-o", "ServerAliveInterval=30", "-o", "StrictHostKeyChecking=accept-new")
    if ($KeyPath) {
        $args = @("-i", $KeyPath) + $args
    }
    return $args
}

function ConvertTo-ShellArg {
    param([string]$Value)
    if ($Value -notmatch "\s") {
        return $Value
    }
    return '"' + ($Value -replace '"', '\"') + '"'
}

function Remove-OldArchives {
    if ($RetentionDays -le 0) {
        return
    }
    $cutoff = (Get-Date).AddDays(-$RetentionDays)
    Get-ChildItem -Path $LocalBackupRoot -Filter "quant_state_*.tar.gz" -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -lt $cutoff } |
        Remove-Item -Force
    Get-ChildItem -Path (Join-Path $LocalBackupRoot "manifests") -Filter "*.json" -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -lt $cutoff } |
        Remove-Item -Force
}

function Invoke-RsyncBackup {
    param([string]$Remote)

    $rsync = Get-Command rsync -ErrorAction SilentlyContinue
    if (-not $rsync) {
        return $false
    }

    $sshArgs = New-SshArgs
    $sshCommand = "ssh " + (($sshArgs | ForEach-Object { ConvertTo-ShellArg $_ }) -join " ")
    foreach ($name in @("data", "models", "logs")) {
        $dst = Join-Path $LocalBackupRoot $name
        New-Item -ItemType Directory -Force -Path $dst | Out-Null
        $src = "${Remote}:$($RemoteStateDir.TrimEnd('/'))/$name/"
        & $rsync.Source -az --delete --partial --info=stats2 -e $sshCommand $src "$dst/"
        if ($LASTEXITCODE -ne 0) {
            throw "rsync failed for $name with exit code $LASTEXITCODE"
        }
    }
    return $true
}

function Invoke-TarFallbackBackup {
    param([string]$Remote)

    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
    $archive = Join-Path $LocalBackupRoot "quant_state_$stamp.tar.gz"
    $remoteArchive = "/tmp/quant_state_$stamp.tar.gz"
    $sshArgs = New-SshArgs
    $scpArgs = New-SshArgs
    $remoteCmd = "set -Eeuo pipefail; rm -f '$remoteArchive'; tar -czf '$remoteArchive' -C '$RemoteStateDir' data models logs; ls -lh '$remoteArchive'"

    & ssh @sshArgs $Remote $remoteCmd
    if ($LASTEXITCODE -ne 0) {
        throw "remote tar creation failed with exit code $LASTEXITCODE"
    }
    & scp @scpArgs "${Remote}:$remoteArchive" $archive
    if ($LASTEXITCODE -ne 0) {
        throw "scp download failed with exit code $LASTEXITCODE"
    }
    & ssh @sshArgs $Remote "rm -f '$remoteArchive'"
    return $archive
}

function Invoke-QuantStateBackup {
    if (-not $Ec2Host) {
        throw "Ec2Host is required. Pass -Ec2Host or set QUANT_AWS_HOST."
    }
    $remote = "$Ec2User@$Ec2Host"
    New-Item -ItemType Directory -Force -Path $LocalBackupRoot | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $LocalBackupRoot "manifests") | Out-Null

    $started = (Get-Date).ToUniversalTime()
    $method = "rsync"
    $archive = $null
    if (-not (Invoke-RsyncBackup -Remote $remote)) {
        $method = "tar_scp_full_archive"
        $archive = Invoke-TarFallbackBackup -Remote $remote
    }

    $manifest = [ordered]@{
        started_utc = $started.ToString("o")
        finished_utc = (Get-Date).ToUniversalTime().ToString("o")
        method = $method
        remote = $remote
        remote_state_dir = $RemoteStateDir
        local_backup_root = (Resolve-Path $LocalBackupRoot).Path
        archive = $archive
    }
    $manifestPath = Join-Path $LocalBackupRoot ("manifests\backup_" + $started.ToString("yyyyMMdd-HHmmss") + ".json")
    $manifest | ConvertTo-Json -Depth 4 | Set-Content -Path $manifestPath -Encoding UTF8
    Remove-OldArchives
    Write-Output "BACKUP_OK method=$method local=$LocalBackupRoot manifest=$manifestPath"
}

do {
    Invoke-QuantStateBackup
    if ($Once -or $IntervalMinutes -le 0) {
        break
    }
    Start-Sleep -Seconds ([Math]::Max(60, $IntervalMinutes * 60))
} while ($true)
