$ErrorActionPreference = 'Stop'

$launcher = Join-Path $PSScriptRoot '..\LaunchEmulator.ps1'
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $launcher,
    [ref]$tokens,
    [ref]$parseErrors)

if ($parseErrors.Count -gt 0) {
    throw "Launcher has PowerShell parse errors: $($parseErrors.Message -join '; ')"
}

$passwordReads = @($ast.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst] -and
        $node.GetCommandName() -eq 'Read-Host' -and
        $node.Extent.Text -match 'password for the SQL Server'
}, $true))

if ($passwordReads.Count -ne 1) {
    throw "Expected one interactive SQL password prompt, found $($passwordReads.Count)."
}
if ($passwordReads[0].Extent.Text -notmatch '(?i)-AsSecureString\b') {
    throw 'The interactive SQL password prompt must use -AsSecureString.'
}

$testPassword = 'Valid\Password1!'
$securePassword = ConvertTo-SecureString $testPassword -AsPlainText -Force
try {
    $convertedPassword =
        [System.Net.NetworkCredential]::new('', $securePassword).Password
}
finally {
    $securePassword.Dispose()
}

if ($convertedPassword -cne $testPassword) {
    throw 'SecureString conversion did not preserve every password character.'
}

Write-Host 'PowerShell launcher password input checks passed.'
