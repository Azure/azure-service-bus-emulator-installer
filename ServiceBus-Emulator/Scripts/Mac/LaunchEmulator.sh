#!/bin/bash

# Initialize variables
ACCEPT_EULA='n'
CONFIG_PATH='../ServiceBus-Emulator/Config/Config.json'
COMPOSE_DOWN='n'
composeFile=$(realpath "$(dirname "$BASH_SOURCE")/../../../Docker-Compose-Template/docker-compose-default.yml")

# Loop through all arguments
for arg in "$@"
do
    if [[ $arg == --ACCEPT_EULA=* ]]; then
        ACCEPT_EULA="${arg#*=}"
        if [[ "$ACCEPT_EULA" != "y" && "$ACCEPT_EULA" != "Y" ]]; then
            # EULA
            echo 'You must accept the EULA (Pass --ACCEPT_EULA="Y" parameter to the script) to continue. Exiting script.'
            exit 1
        fi
    fi

    if [[ $arg == --CONFIG_PATH=* ]]; then
        CONFIG_PATH="${arg#*=}"
    fi

    if [[ $arg == --compose-down=* ]]; then
        COMPOSE_DOWN="${arg#*=}"
        if [[ "$COMPOSE_DOWN" != "y" && "$COMPOSE_DOWN" != "Y" ]]; then
            echo 'Invalid value for --compose-down. Use "Y" to only run docker compose down. Exiting script.'
            exit 1
        fi
    fi
done

# Skip EULA check if only running docker compose down
if [[ "$COMPOSE_DOWN" != 'y' && "$COMPOSE_DOWN" != 'Y' ]]; then
    # Check if ACCEPT_EULA is 'Y' or 'y'
    if [[ "$ACCEPT_EULA" != "y" && "$ACCEPT_EULA" != "Y" ]]; then
        # EULA
        echo 'By pressing "Y", you are expressing your consent to the End User License Agreement (EULA) for Event-Hubs Emulator: https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt'
        read ACCEPT_EULA
        if [[ "$ACCEPT_EULA" != "y" && "$ACCEPT_EULA" != "Y" ]]; then
            echo "You must accept the EULA (Press 'Y') to continue. Exiting script."
            exit 1
        fi
    fi

    # Set EULA as env variable
    echo "EULA has been accepted. Proceeding with launching containers.."
    export ACCEPT_EULA=$ACCEPT_EULA
fi

# Set Config Path as env variable
export CONFIG_PATH=$CONFIG_PATH

# Run docker compose down
docker compose -f $composeFile down

if [ $? -ne 0 ]; then
    echo "An error occurred while running docker compose down. Exiting the script."
    exit 1
fi

# If --compose-down is not 'Y', proceed with bringing containers up
if [[ "$COMPOSE_DOWN" != 'y' && "$COMPOSE_DOWN" != 'Y' ]]; then
    docker compose -f $composeFile up -d

    if [ $? -ne 0 ]; then
        echo "An error occurred while running docker compose up. Exiting the script."
        exit 1
    fi

    echo "Emulator Service and dependencies have been successfully launched!"
else
    echo "Docker compose down completed. Skipping docker compose up as --compose-down='Y' was passed."
fi