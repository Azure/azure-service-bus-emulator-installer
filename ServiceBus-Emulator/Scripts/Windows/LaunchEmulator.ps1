param(
  [string]$ACCEPT_EULA='n',
  [string]$CONFIG_PATH='../ServiceBus-Emulator/Config/Config.json',
  [string]$SQL_PASSWORD=''
)

Write-Warning "As running native .ps1 script required updating your machine's execution policy,
         the .ps1 script will be phased out by June 2025. To mitigate this, please transition
         to running .sh script via WSL on Windows. You can find the Launchemulator.sh script in
         the Common folder: https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/ServiceBus-Emulator/Scripts/Common/LaunchEmulator.sh"

# For dynamic ports and support communication to host network use the commented docker compose file path instead.
# composeFile=$(realpath "$(dirname "$BASH_SOURCE")/../../../Docker-Compose-Template/docker-compose-custom-ports-windows-mac.yaml")
$composeFile = Join-Path $PSScriptRoot "/../../../Docker-Compose-Template/docker-compose-default.yml"

if ($PSBoundParameters.ContainsKey('ACCEPT_EULA')) {
    if ($ACCEPT_EULA -ne 'y' -and $ACCEPT_EULA -ne 'Y') {
        Write-Host "You must accept the EULA (Pass --ACCEPT_EULA 'Y' parameter to the script) to continue. Exiting script."
        exit
    }
}
else{
    # EULA
    $ACCEPT_EULA = Read-Host 'By pressing "Y", you are expressing your consent to the End User License Agreement (EULA) for Service-Bus Emulator: https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt and Azure SQL Edge : https://go.microsoft.com/fwlink/?linkid=2139274'
    if ($ACCEPT_EULA -ne 'y' -and $ACCEPT_EULA -ne 'Y') {
        Write-Host "You must accept the EULA (Press 'Y') to continue. Exiting script."
        exit
    }
}

function Validate-Password {
    param(
        [string]$password
    )

    $uppercasePattern = '(?-i)[A-Z]'
    $lowercasePattern = '(?-i)[a-z]'
    $digitPattern = '\d'
    $specialCharPattern = '[\W_]'
    $charAllowed = '[A-Za-z\d\W_]{8,128}'


    $matchCount = 0
    if($password -match $uppercasePattern){
        $matchCount++
    }
    if($password -match $lowercasePattern){
        $matchCount++
    }
    if($password -match $digitPattern){
        $matchCount++
    }
    if($password -match $specialCharPattern){
        $matchCount++
    }

    if($matchCount -lt 3 -or $password -notmatch $charAllowed){
        return $false
    }
    return $true
}

if($PSBoundParameters.ContainsKey('SQL_PASSWORD')){

    $isValid = Validate-Password -password $SQL_PASSWORD

    if(-not $isValid){
        Write-Host "Invalid password. Password must meet the security requirements : https://learn.microsoft.com/en-us/sql/relational-databases/security/strong-passwords?view=sql-server-linux-ver16"
        exit
    }
}
else{
    $SQL_PASSWORD = Read-Host "Enter the password for the SQL Server (To be filled as per policy : https://learn.microsoft.com/en-us/sql/relational-databases/security/strong-passwords?view=sql-server-linux-ver16)"

    $isValid = Validate-Password -password $SQL_PASSWORD

    if(-not $isValid){
        Write-Host "Invalid password. Password must meet the security requirements : https://learn.microsoft.com/en-us/sql/relational-databases/security/strong-passwords?view=sql-server-linux-ver16"
        exit
    }
}

# Set EULA as env variable
Write-Host "EULA has been accepted. Proceeding with launching containers.."
$env:ACCEPT_EULA = $ACCEPT_EULA

# Set SQL Password as env variable
$env:SQL_PASSWORD = $SQL_PASSWORD

# Set Config Path as env variable
$env:CONFIG_PATH = $CONFIG_PATH

# Run Docker Compose
docker compose -f $composeFile down
docker compose -f $composeFile up -d

if ($LASTEXITCODE -ne 0) {
    Write-Output "An error occurred while running docker compose.Exiting the script."
    exit 1
}

Write-Host "Emulator Service and dependencies have been successfully launched!" -ForegroundColor Green