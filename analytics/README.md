# Setup Analytics Pipeline

![Architecture](https://raw.githubusercontent.com/jomit/iot-central/master/images/analytics.png)

### Prerequisites

- An extended 30-day trial [IoT Central application](https://apps.azureiotcentral.com/), or a paid application.

- Install [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest)

- Install [Power BI Desktop](https://powerbi.microsoft.com/en-us/blog/power-bi-desktop-october-2018-feature-summary/)

### Azure Blob Storage

- Create a new [Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=portal) and copy the Connection string.

- Create a new container in the Blob named `data`.

### Azure SQL DB

- Create a new Standard (S1) [Azure SQL DB](https://docs.microsoft.com/en-us/azure/sql-database/sql-database-get-started-portal) and copy the Connection string.

- Execute all the scripts from `analytics\sqldb` folder, in the respective order.

### Azure Function App

- Create a new Function App using ARM Template

    - Update values in `analytics\functionapp\arm\parameters.json`
    - Deploy the ARM Template:
        - `az login`
        - `az group deployment create --name "DeployAnalyticsFunctionapp" --resource-group "<your-resource-group>" --template-file .\template.json --parameters .\parameters.json`

- Create 3 new C# Blog trigger functions as follows:

    - Devices
        - Name : `devices` 
        - Code : `analytics\functionapp\code\devices`
    - Device Templates
        - Name : `deviceTemplates`
        - Code : `analytics\functionapp\code\deviceTemplates`
    - Measurements
        - Name : `measurements`
        - Code : `analytics\functionapp\code\measurements`

### Azure Data Factory

- Create a new Data Factory pipeline using ARM Template

    - Update values in `analytics\datafactory\parameters.json`
    - Deploy the ARM Template:

        - `az login`
        - `az group deployment create --name "DeployDataFactory" --resource-group "<your-resource-group>" --template-file .\template.json --parameters .\parameters.json`

- Start the Trigger

    - Open Data Factory in Azure Portal and click on `Author & Monitor Portal`.
    - Select `Author` from the left navigation and click on `Pipelines` in the right section, you should see a new pipline named `ETL`.
    - Select `ETL` and click on `Trigger` tab, you should see a trigger named `DefaultTrigger`.
    - Under the `Actions` column, click on the `Activate` button and then hit Publish All.
    - To see the output, select `Monitor` from the left navigation to see all the Runs.

### Enable Continuous Export

- Setup Continuous [Data Export in IoT Central](https://docs.microsoft.com/en-us/azure/iot-central/howto-export-data) to Azure Blob Storage and container `data` created above.

### PowerBI

- Open the `analytics\powerbi\IoT-Continuous-Data-Export-Template.pbix` file in PowerBI Desktop and follow the instructions to update the query, data source and refresh the report

- Devices and Measurements Dashboard
![PowerBI Dashboard Devices](https://raw.githubusercontent.com/jomit/iot-central/master/images/pbi-1.png)

- Events Dashboard
![PowerBI Dashboard Events](https://raw.githubusercontent.com/jomit/iot-central/master/images/pbi-2.png)


### Additional Resources

- [Device Connectivity Monitoring using Azure IoT Explorer](https://docs.microsoft.com/en-us/azure/iot-central/howto-use-iotc-explorer)

- [Visualize and analyze your Azure IoT Central data in a Power BI dashboard](https://docs.microsoft.com/en-us/azure/iot-central/howto-connect-powerbi)
