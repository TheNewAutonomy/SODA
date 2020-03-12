using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Linq;

namespace DataAccess
{
    public class DMAMeterReadingTableStorageContext
    {
        CloudTable meterTable;

        public DMAMeterReadingTableStorageContext(string tableName)
        {
            var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
            var tableClient = account.CreateCloudTableClient();
            meterTable = tableClient.GetTableReference(tableName);
            meterTable.CreateIfNotExists();
        }

        public DMAMeterReadingEntity MeterReading(string id, string readingDateTime)
        {
            var retrieveOperation = TableOperation.Retrieve<DMAMeterReadingEntity>(id, readingDateTime);

            // Execute the retrieve operation.
            var retrievedResult = meterTable.Execute(retrieveOperation);

            return (DMAMeterReadingEntity) retrievedResult.Result;
        }

        public IQueryable<DMAMeterReadingEntity> MeterReadings => meterTable.CreateQuery<DMAMeterReadingEntity>();

        public List<DMAMeterReadingEntity> MeterReadingsFiltered(string finalFilter)
        {
            var query = new TableQuery<DMAMeterReadingEntity>().Where(finalFilter);
            return meterTable.ExecuteQuery(query).ToList();
        }
    }
}
