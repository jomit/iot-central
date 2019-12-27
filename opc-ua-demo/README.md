# Demo: Azure OPC Components connected with IoT Central 
##### (Note: These instructions are using Powershell and Linux Containers on Windows)

### Setup

- [Create an IoT Central App](https://docs.microsoft.com/en-us/azure/iot-central/core/quick-deploy-iot-central)

- Create new [custom device template](https://docs.microsoft.com/en-us/azure/iot-central/core/tutorial-define-device-type#create-a-device-template)
    - Add new Telemetry Measurement with following details:

      Display Name: Speed

      Field Name: StepUp

- [Add a Real Device](https://docs.microsoft.com/en-us/azure/iot-central/core/tutorial-add-device#add-a-real-device)

- Get Device Connection String 
    - `npm i -g dps-keygen`
    - `dps-keygen -di:<device id> -dk:<device key> -si:<device scope id>`

- Create Environment Variables
    - `$env:_DEVICE_CS = "<iot central device connection string>"`
    - `$env:_REPO_ROOT = "<fully qualified directory name of current directory>`

    - `Get-ChildItem  Env:_DEVICE_CS`
    - `Get-ChildItem  Env:_REPO_ROOT`

### Deploy

- `docker-compose -f .\opcserver-publisher.yml up`

### Test

- Install an OPC UA Client (e.g. [UAExpert](https://www.unified-automation.com/downloads/opc-ua-clients.html))

- Right click the `Server` and click `Add`

    ![Add Server](https://raw.githubusercontent.com/jomit/iot-central/master/images/add-server.png)

    ![Add Server Config](https://raw.githubusercontent.com/jomit/iot-central/master/images/add-server-config.png)

- Click `OK` on the "Url not found" message box.

- Click `Trust Server Certificate` and `Continue`

    ![Trust Certificate](https://raw.githubusercontent.com/jomit/iot-central/master/images/trust-certificate.png)

- Drag the `StepUp` variable into `Data Access View` to see the current Value

    ![StepUp Data Access View](https://raw.githubusercontent.com/jomit/iot-central/master/images/stepup-data-access.png)

- Call the `ResetStepUp` method to reset the StepUp value

    ![Reset StepUp](https://raw.githubusercontent.com/jomit/iot-central/master/images/reset-stepup.png)

- Observe the Telemetry Chart in IoT Central. 

    ![Device Telemetry](https://raw.githubusercontent.com/jomit/iot-central/master/images/device-telemetry.png)
