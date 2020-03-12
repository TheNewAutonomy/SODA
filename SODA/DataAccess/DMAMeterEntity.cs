using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace DataAccess
{
    public class DMAMeterEntity : TableEntity
    {
        // By default, when creating a new entity, the PartitionKey is set to the current year, and the RowKey is a GUID. Insert the ticks in the beginning of RowKey because the result returned by a query is ordered by PartitionKey and then RowKey. 
        public DMAMeterEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public DMAMeterEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }
    }

}
