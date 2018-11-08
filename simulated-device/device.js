'use strict';
var clientFromConnectionString = require('azure-iot-device-http').clientFromConnectionString;
var Message = require('azure-iot-device').Message;
var connectionString = '[IoT Hub device connection string]';
var client = clientFromConnectionString(connectionString);

function getRandomeVolume(low, high) {
    return Math.floor(Math.random() * (high - low + 1) + low);
}

setInterval(function(){
  var volume = getRandomeVolume(2000,7000);
  var message = new Message(JSON.stringify({
    volume: volume
  }));

  console.log('Sending message: ' + message.getData());

  client.sendEvent(message, function (err) {
    if (err) {
      console.error('send error: ' + err.toString());
    } else {
      console.log('message sent');
    }
  });
}, 1000);