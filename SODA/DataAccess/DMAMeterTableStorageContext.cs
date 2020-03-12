using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Linq;

namespace DataAccess
{
    public class DMAMeterTableStorageContext
    {
        CloudTable meterTable;

        public DMAMeterTableStorageContext(string tableName)
        {
            var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
            var tableClient = account.CreateCloudTableClient();
            meterTable = tableClient.GetTableReference(tableName);
            meterTable.CreateIfNotExists();
        }

        public DMAMeterEntity MeterReading(string id, string readingDateTime)
        {
            var retrieveOperation = TableOperation.Retrieve<DMAMeterEntity>(id, readingDateTime);

            // Execute the retrieve operation.
            var retrievedResult = meterTable.Execute(retrieveOperation);

            return (DMAMeterEntity) retrievedResult.Result;
        }

        public IQueryable<DMAMeterEntity> MeterReadings => meterTable.CreateQuery<DMAMeterEntity>();

        public List<DMAMeterEntity> MeterReadingsFiltered(string finalFilter)
        {
            var query = new TableQuery<DMAMeterEntity>().Where(finalFilter);
            return meterTable.ExecuteQuery(query).ToList();
        }
    }
}
