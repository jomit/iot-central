using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;

namespace RSLinxConnectApp
{
    class Program
    {
        private readonly static int delayInMilliseconds = 10000;

        static void Main(string[] args)
        {
            var topicConfig = GetTopicConfig();
            var topicItems = topicConfig.TopicItems;

            var opcHelper = new OPCDaHelper();
            var connected = opcHelper.Connect("");

            if (connected)
            {
                Console.WriteLine(opcHelper.VendorInfo);
                Console.WriteLine(opcHelper.VersionInfo);
                Console.WriteLine(opcHelper.StatusInfo);

                var daItems = new List<Opc.Da.Item>();
                foreach (var item in topicItems)
                {
                    daItems.Add(new Opc.Da.Item(new Opc.ItemIdentifier(null, item.Name)) {
                        ReqType = null,
                        MaxAge = 0,
                        MaxAgeSpecified = false,
                        Active =false,
                        ActiveSpecified = false,
                        Deadband = 0,
                        DeadbandSpecified = false,
                        SamplingRate = 0,
                        SamplingRateSpecified = false,
                        ClientHandle = Guid.NewGuid().ToString()
                    });
                }
                opcHelper.AddTopicItems(daItems);

                StartReadAsync(opcHelper, topicItems);
            }
            Console.ReadLine();
        }

        static TopicConfig GetTopicConfig()
        {
            TopicConfig config;
            using (var r = new StreamReader("config.json"))
            {
                var json = r.ReadToEnd();
                config = JsonConvert.DeserializeObject<TopicConfig>(json);
            }
            return config;
        }

        static async void StartReadAsync(OPCDaHelper opcHelper, List<TopicItem> topicItems)
        {
            while (true)
            {
                var resultItems = opcHelper.Read();
                foreach (var item in resultItems)
                {
                    var currentTopicItem = topicItems.SingleOrDefault(i => i.Name == item.ItemName);
                    Console.WriteLine("Topic Found => " + currentTopicItem.Name);
                    //Task.Run(async () => await SendDataToIoTCentralAsync(currentTopicItem.DeviceConnectionString, currentTopicItem.DeviceTelemetryFieldName, item.ItemName, item.Value.ToString())).ConfigureAwait(false);
                    await SendDataToIoTCentralAsync(currentTopicItem.DeviceConnectionString,
                        currentTopicItem.DeviceTelemetryFieldName,
                        item.ItemName,
                        item.Value.ToString());
                }
                await Task.Delay(delayInMilliseconds);
            }
        }

        static async Task SendDataToIoTCentralAsync(string deviceConnectionString, string deviceTelemetryFieldName, string name, string value)
        {
            using (var client = DeviceClient.CreateFromConnectionString(deviceConnectionString, TransportType.Http1))
            {
                var messageString = "{ \"" + deviceTelemetryFieldName + "\" :" + value + "}";
                var message = new Message(Encoding.ASCII.GetBytes(messageString));
                await client.SendEventAsync(message);
                Console.WriteLine("{0} | {1} > {2}", DateTime.Now, name, messageString);
            }
        }
    }
}
