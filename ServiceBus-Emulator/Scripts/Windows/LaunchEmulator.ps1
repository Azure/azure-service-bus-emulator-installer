param(
  [string]$ACCEPT_EULA='n',
  [string]$CONFIG_PATH='../ServiceBus-Emulator/Config/Config.json'
)

# For dynamic ports and support communication to host network use the commentted docker compose file path instead.
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
    $ACCEPT_EULA = Read-Host 'By pressing "Y", you are expressing your consent to the End User License Agreement (EULA) for Event-Hubs Emulator: https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt'
    if ($ACCEPT_EULA -ne 'y' -and $ACCEPT_EULA -ne 'Y') {
        Write-Host "You must accept the EULA (Press 'Y') to continue. Exiting script."
        exit
    }
}

# Set EULA as env variable
Write-Host "EULA has been accepted. Proceeding with launching containers.."
$env:ACCEPT_EULA = $ACCEPT_EULA

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