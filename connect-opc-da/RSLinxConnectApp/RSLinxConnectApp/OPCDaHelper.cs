using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RSLinxConnectApp
{
    public class OPCDaHelper
    {
        public string VendorInfo { get; private set; }
        public string VersionInfo { get; private set; }
        public string StatusInfo { get; private set; }
        public Opc.Da.Server Server { get; private set; }
        public Opc.Da.Subscription Subscription { get; private set; }

        public bool Connect(string serverUrl)
        {
            var opcServerUrl = new Opc.URL(serverUrl);
            Server = new Opc.Da.Server(new OpcCom.Factory(), opcServerUrl);

            try
            {
                //System.Net.NetworkCredential credentials = null;
                //System.Net.WebProxy webProxy = null;
                var connectData = new Opc.ConnectData(null, null);
                Server.Connect(connectData);

                var status = Server.GetStatus();

                VendorInfo = status.VendorInfo;
                VersionInfo = status.ProductVersion;
                StatusInfo = status.StatusInfo;

                // Assign a globally unique handle to the subscription.
                var state = new Opc.Da.SubscriptionState()
                {
                    Name = "RSLinxConnectApp",
                    Active = false,
                    UpdateRate = 1000,
                    KeepAlive = 0,
                    Deadband = 0,
                    Locale = null,
                    ClientHandle = Guid.NewGuid().ToString(),
                    ServerHandle = null
                };

                Subscription = Server.CreateSubscription(state) as Opc.Da.Subscription;

                return true;
            }
            catch (Exception e)
            {
                StatusInfo = e.Message;
                Console.WriteLine(e.Message);
            }
            return false;
        }

        public bool AddTopicItems(List<Opc.Da.Item> topicItemList)
        {
            Subscription.AddItems(topicItemList.ToArray());
            return true;
        }

        public Opc.Da.ItemValueResult[] Read()
        {
            try
            {
                return Subscription.Read(Subscription.Items);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }
    }
}
