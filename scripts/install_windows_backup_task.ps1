param(
    [string]$Ec2Host = $env:QUANT_AWS_HOST,
    [string]$Ec2User = $(if ($env:QUANT_AWS_USER) { $env:QUANT_AWS_USER } else { "ec2-user" }),
    [string]$KeyPath = $env:QUANT_AWS_KEY,
    [string]$RemoteStateDir = $(if ($env:QUANT_AWS_STATE_DIR) { $env:QUANT_AWS_STATE_DIR } else { "/home/ec2-user/quant/state" }),
    [string]$LocalBackupRoot = $(if ($env:QUANT_LOCAL_BACKUP_ROOT) { $env:QUANT_LOCAL_BACKUP_ROOT } else { (Join-Path $PSScriptRoot "..\aws_state_backups") }),
    [int]$EveryMinutes = 15,
    [string]$TaskName = "QuantAwsStateBackup"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $Ec2Host) {
    throw "Ec2Host is required. Pass -Ec2Host or set QUANT_AWS_HOST."
}
if ($EveryMinutes -lt 5) {
    throw "EveryMinutes must be at least 5."
}

$backupScript = Resolve-Path (Join-Path $PSScriptRoot "aws_pull_state_backup.ps1")
$argList = @(
    "-NoProfile",
    "-WindowStyle", "Hidden",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$backupScript`"",
    "-Ec2Host", "`"$Ec2Host`"",
    "-Ec2User", "`"$Ec2User`"",
    "-RemoteStateDir", "`"$RemoteStateDir`"",
    "-LocalBackupRoot", "`"$LocalBackupRoot`"",
    "-Once"
)
if ($KeyPath) {
    $argList += @("-KeyPath", "`"$KeyPath`"")
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument ($argList -join " ")
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes $EveryMinutes)
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew -StartWhenAvailable

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Description "Pull Quant bot state/models/data/logs from AWS to this laptop." -Force | Out-Null

Write-Output "TASK_INSTALLED name=$TaskName every_minutes=$EveryMinutes"
Write-Output "Run once now with:"
Write-Output "powershell -NoProfile -ExecutionPolicy Bypass -File `"$backupScript`" -Ec2Host `"$Ec2Host`" -Ec2User `"$Ec2User`" -KeyPath `"$KeyPath`" -RemoteStateDir `"$RemoteStateDir`" -LocalBackupRoot `"$LocalBackupRoot`" -Once"
