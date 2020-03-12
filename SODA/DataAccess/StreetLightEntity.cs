using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace TableDataAccess
{
    public class StreetLightEntity : TableEntity
    {
        public StreetLightEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public StreetLightEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string DeviceID { get; set; }
        public string IPAddress { get; set; }
        public string Temperature { get; set; }
        public string Light { get; set; }
        public DateTime DeviceTime { get; set; }
    }
}
