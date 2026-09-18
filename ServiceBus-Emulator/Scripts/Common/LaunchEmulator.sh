#!/bin/bash

# Initialize variables
ACCEPT_EULA='n'
CONFIG_PATH='../ServiceBus-Emulator/Config/Config.json'
COMPOSE_DOWN='n'
composeFile=$(realpath "$(dirname "$BASH_SOURCE")/../../../Docker-Compose-Template/docker-compose-default.yml")
SQL_PASSWORD=''
SQL_WAIT_INTERVAL=''

# Password regex pattern
char_allowed='^.{8,128}$'
digit_pattern='[0-9]'
special_char_pattern='[^a-zA-Z0-9]'
uppercase_pattern='[A-Z]'
lowercase_pattern='[a-z]'

validate_password() {
    local count=0
    if [[ "$SQL_PASSWORD" =~ $digit_pattern ]]; then
        count=$((count+1))
    fi

    if [[ "$SQL_PASSWORD" =~ $special_char_pattern ]]; then
        count=$((count+1))
    fi

    if [[ "$SQL_PASSWORD" =~ $uppercase_pattern ]]; then
        count=$((count+1))
    fi

    if [[ "$SQL_PASSWORD" =~ $lowercase_pattern ]]; then
        count=$((count+1))
    fi

    if [[ "$SQL_PASSWORD" =~ $char_allowed ]] && [[ $count -ge 3 ]]; then
        return 1
    else
        return 0
    fi
}


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

    if [[ $arg == --SQL_PASSWORD=* ]]; then
        SQL_PASSWORD="${arg#*=}"

        validate_password
        result=$?

        if [[ $result -eq 0 ]]; then
            echo 'Invalid password. Password must meet the security requirements : https://learn.microsoft.com/en-us/sql/relational-databases/security/strong-passwords?view=sql-server-linux-ver16'
            exit 1
        fi
    fi

done

# Skip EULA check if only running docker compose down
if [[ "$COMPOSE_DOWN" != 'y' && "$COMPOSE_DOWN" != 'Y' ]]; then
    # Check if ACCEPT_EULA is 'Y' or 'y'
    if [[ "$ACCEPT_EULA" != "y" && "$ACCEPT_EULA" != "Y" ]]; then
        # EULA
        echo 'By pressing "Y", you are expressing your consent to the End User License Agreement (EULA) for Service-Bus Emulator: https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt and Azure SQL Edge : https://go.microsoft.com/fwlink/?linkid=2139274'
        read ACCEPT_EULA
        if [[ "$ACCEPT_EULA" != "y" && "$ACCEPT_EULA" != "Y" ]]; then
            echo "You must accept the EULA (Press 'Y') to continue. Exiting script."
            exit 1
        fi
    fi

    validate_password
    result=$?

    if [[ $result -eq 0 ]]; then
        echo 'Enter the password for the SQL Server (To be filled as per policy : https://learn.microsoft.com/en-us/sql/relational-databases/security/strong-passwords?view=sql-server-linux-ver16)'
        read SQL_PASSWORD

        validate_password
        result=$?

        if [[ $result  -eq 0 ]]; then
            echo 'Invalid password. Password must meet the security requirements : https://learn.microsoft.com/en-us/sql/relational-databases/security/strong-passwords?view=sql-server-linux-ver16'
            exit 1
        fi
    fi

    if [ -z "$SQL_WAIT_INTERVAL" ]; then
        read -p "Enter the wait interval for SQL Server to be ready (in seconds) [default: 15]: " SQL_WAIT_INTERVAL
        # Set default value if no input is provided
        SQL_WAIT_INTERVAL=${SQL_WAIT_INTERVAL:-15}
    fi

    if [ -z "$EMULATOR_HTTP_PORT" ]; then
        read -p "Enter the emulator HTTP port for health-check and Management APIs [default: 5300]: " EMULATOR_HTTP_PORT
        # Set default value if no input is provided
        EMULATOR_HTTP_PORT=${EMULATOR_HTTP_PORT:-5300}
    fi

    # Set EULA as env variable
    echo "EULA has been accepted. Proceeding with launching containers.."
    export ACCEPT_EULA=$ACCEPT_EULA
    export SQL_PASSWORD=$SQL_PASSWORD
    export SQL_WAIT_INTERVAL=$SQL_WAIT_INTERVAL
    export EMULATOR_HTTP_PORT=$EMULATOR_HTTP_PORT
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