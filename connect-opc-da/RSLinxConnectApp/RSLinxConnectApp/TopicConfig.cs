using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RSLinxConnectApp
{
    public class TopicConfig
    {
        public string OpcServerUrl { get; set; }

        public List<TopicItem> TopicItems { get; set; }
    }

    public class TopicItem
    {
        public string Name { get; set; }

        public string DeviceConnectionString { get; set; }

        public string DeviceTelemetryFieldName { get; set; }

        public string Value { get; set; }
    }
}
