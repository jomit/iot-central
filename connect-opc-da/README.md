# Connect with RSLinx (OPC DA)

![Architecture](https://raw.githubusercontent.com/jomit/iot-central/master/images/connectopcda.png)

### Prerequisites

- A Machine with RSLinx installed and connected with the PLC

- Install [OPC Classic Core Components](https://opcfoundation.org/developer-tools/samples-and-tools-classic/core-components/)

- An [IoT Central application](https://apps.azureiotcentral.com/).

- Install [Visual Studio 2017](https://docs.microsoft.com/en-us/visualstudio/install/install-visual-studio?view=vs-2017)

- Install [Azure IoT DPS Symmetric Key Generator](https://www.npmjs.com/package/dps-keygen)

### Device Type

- [Create new Device Type](https://docs.microsoft.com/en-us/azure/iot-central/tutorial-define-device-type)

    - Configure Telemetry Measurements and save the `Field Name` as we will need this later to configure the RSLinxConnectorApp.

    - Remove the default simulated device created for you.
### Device

- [Add new Real Device](https://docs.microsoft.com/en-us/azure/iot-central/tutorial-add-device)

- Get the connection string of the device using the `dps-keygen` tool as shown [here](https://docs.microsoft.com/en-us/azure/iot-central/concepts-connectivity#getting-device-connection-string)

    - For windows you may need to copy files from `...<USER>\AppData\Roaming\npm\node_modules\dps-keygen\bin\windows` to your system path.

    - `npm i -g dps-keygen`

    - `dps_cstr <scope_id> <device_id> <Primary Key(for device)>`

### Telemetry Rule (Optional)

- [Create new Telemetry Rule](https://docs.microsoft.com/en-us/azure/iot-central/howto-create-telemetry-rules) (if needed)

### Configure and Run RXLinxConnectApp

- Open `connect-opc-da\RSLinxConnectApp\RSLinxConnectApp.sln` in Visual Studio 2017

- Update the `config.json` file:
    - `opcServerUrl`    :   URL of the OPC DA Server along with the GUID
    - `topicItems -> name`  :   Topic Name and Item Name configured in RSLinx
    - `topicItems -> deviceConnectionString`    :   Connection string of the IoT Central device
    - `topicItems -> deviceTelemetryFieldName`  :   Field Name from the IoT Central Device Template telemetry measurement.

- Run the Solution

- Open the Device Dashboard in IoT Central to see the real time chart of the measurements and/or setup [monintoring](https://docs.microsoft.com/en-us/azure/iot-central/tutorial-monitor-devices)

### Additional Resources

- [Azure IoT Central Analytics](https://docs.microsoft.com/en-us/azure/iot-central/howto-create-analytics)

- [Setup Analytics Pipeline](https://github.com/jomit/iot-central/blob/master/analytics)