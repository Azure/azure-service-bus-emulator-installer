
 # <img src="https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/azure-servicebus-emulator.svg" alt="Event-Hubs Logo" width="50">Azure Service Bus Emulator Installer

This repository contains the scripts required to install and run the  [Azure Service Bus Emulator](https://learn.microsoft.com/en-us/azure/service-bus-messaging/overview-emulator).

- [Azure Service Bus Emulator Installer](#azure-service-bus-emulator-installer)
  - [About Azure Service Bus](#about-azure-service-bus)
  - [About Azure Service Bus emulator](#about-azure-service-bus-emulator)
  - [Run Azure Service Bus Emulator](#run-azure-service-bus-emulator)
  - [Prerequisites](#prerequisites)
  - [Running the Emulator](#running-the-emulator)
      - [Using Automated Script](#using-automated-script)
    - [Windows](#windows)
    - [Linux \& macOS](#linux--macos)
      - [Using Docker Compose (Linux Container)](#using-docker-compose-linux-container)
  - [Interacting with the emulator](#interacting-with-the-emulator)
  - [Support](#support)
  - [License](#license)

## About Azure Service Bus

Azure Service Bus is a fully managed enterprise message broker offering queues and publish-subscribe topics. It decouples applications and services, providing benefits like load-balancing across workers, safe data and control routing, and reliable transactional coordination. Read more [here](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-overview).

## About Azure Service Bus emulator 

The Azure Service Bus emulator offers a local development experience for the Service bus service. You can use the emulator to develop and test code against the service in isolation, free from cloud interference.

>[!CAUTION]
>Emulator is intended solely for development and testing scenarios.Any kind of Production use is strictly discouraged. There is no official support provided for Emulator.
> Any issues/suggestions should be reported via GitHub issues on [GitHub project](https://github.com/Azure/azure-service-bus-emulator-installer/issues).
## Run Azure Service Bus Emulator 

This section summarizes the steps to develop and test locally with Service Bus Emulator. To read more about Service Bus, read [here](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-overview).

## Prerequisites

- Docker 
  - [Docker Desktop](https://docs.docker.com/desktop/install/windows-install/#:~:text=Install%20Docker%20Desktop%20on%20Windows%201%20Download%20the,on%20your%20choice%20of%20backend.%20...%20More%20items) 
- Minimum hardware Requirements:
  - 2 GB RAM
  - 5 GB of Disk space
- WSL Enablement (Only for Windows):
  - [Install Windows Subsystem for Linux (WSL) | Microsoft Learn](https://learn.microsoft.com/en-us/windows/wsl/install)
  -  [Configure Docker to use WSL](https://docs.docker.com/desktop/wsl/#:~:text=Turn%20on%20Docker%20Desktop%20WSL%202%201%20Download,engine%20..%20...%206%20Select%20Apply%20%26%20Restart.)

>[!NOTE]
>Before you continue with the subsequent steps, make sure Docker Engine is operational in the background.

## Running the Emulator 

This section highlights different steps to run Service Bus Emulator. Details are as follows:

#### [Using Automated Script](#tab/automated-script)

Before running automated script, clone the installation [repository](https://github.com/Azure/azure-service-bus-emulator-installer) locally.
 
### Windows
After completing the prerequisites, you can proceed with the following steps to run the Service Bus Emulator locally. 

1. Before executing the setup script, we need to allow execution of unsigned scripts. Run the below command in the PowerShell window:

`$>Start-Process powershell -Verb RunAs -ArgumentList 'Set-ExecutionPolicy Bypass –Scope CurrentUser’`

2. Execute setup script `LaunchEmulator.ps1`. Running the script would bring up two containers – Service Bus Emulator & Azure SQL Edge (dependency for Emulator)


### Linux & macOS
After completing the prerequisites, you can proceed with the following steps to run the Service Bus Emulator locally. 


1. Execute the setup script `LaunchEmulator.sh` . Running the script would  bring up two containers – Service Bus Emulator & Azure SQL Edge (dependency for Emulator)


2. Execute the same script `LaunchEmulator.sh` with the option `--compose-down=Y` to issue a `docker compose down` to terminate the containers.

```shell
LaunchEmulator.sh --compose-down=Y
```

#### [Using Docker Compose (Linux Container)](#tab/docker-linux-container)


You can also spin up Emulator using Docker Compose file directly. Refer [here](https://learn.microsoft.com/en-us/azure/service-bus-messaging/test-locally-with-service-bus-emulator#tabs=docker-linux-container) for details:






Once the steps are successful, Emulator compose set can be found running in Docker.

![test-locally-with-service-bus-emulator](https://github.com/user-attachments/assets/0792b294-a3dc-4980-aca2-ec4db04125a6)

## Interacting with the emulator

By default, emulator uses [config.json](https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/ServiceBus-Emulator/Config/Config.json) configuration file. You can configure entities by making changes to configuration file. To know more, visit [make configuration changes](https://learn.microsoft.com/en-us/azure/service-bus-messaging/overview-emulator#quota-configuration-changes). 

>[!NOTE]
> Service Bus emulator isn't compatible with the community owned [open source Service Bus Explorer](https://github.com/paolosalvatori/ServiceBusExplorer)

You can use the following connection string to connect to the Service Bus emulator:

 - When the emulator container and interacting application are running natively on local machine, use following connection string:
```
"Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
```
  - Applications (Containerized/Non-containerized) on the different machine and same local network can interact with Emulator using the IPv4 address of the machine. Use following connection string:
```
"Endpoint=sb://192.168.y.z;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
```
  - Application containers on the same bridge network can interact with Emulator using its alias or IP. Following connection string assumes the name of Emulator container is "servicebus-emulator":
```
"Endpoint=sb://servicebus-emulator;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
```
  - Application containers on the different bridge network can interact with Emulator using the "host.docker.internal" as host. Use following connection string:
```
"Endpoint=sb://host.docker.internal;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
```

You can use the latest client SDKs to interact with the Service Bus emulator across various programming languages. To get started, refer to the [Service Bus emulator samples on GitHub](https://github.com/Azure/azure-service-bus-emulator-installer/tree/main/Sample-Code-Snippets/NET/ServiceBus.Emulator.Console.Sample).



## Support

There is no official support provided for Emulator.Any issues/suggestions should be reported via GitHub issues on [installation repo](https://github.com/Azure/azure-service-bus-emulator-installer/issues).

## License

The scripts and documentation in this project are released under the MIT License.

The software (Azure Service Bus Emulator and Sql Edge) that the scripts in this repository install are licensed under separate terms. Refer to the terms governing each software below:
- Azure Service Bus emulator : [EULA](https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt)
- Azure SQL Edge Developer Edition: [EULA](https://go.microsoft.com/fwlink/?linkid=2139274) 
  - SQL Edge provides Developer and Premium editions, Service Bus Emulator relies on the Developer edition which is licensed for use as a development and test system and cannot be used in a production environment.







   


