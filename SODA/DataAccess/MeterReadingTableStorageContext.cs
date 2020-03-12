using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Linq;

namespace DataAccess
{
    public class MeterReadingTableStorageContext
    {
        CloudTable meterTable;

        public MeterReadingTableStorageContext(string tableName)
        {
            var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
            var tableClient = account.CreateCloudTableClient();
            meterTable = tableClient.GetTableReference(tableName);
            meterTable.CreateIfNotExists();
        }

        public MeterReadingEntity MeterReading(string id, string readingDateTime)
        {
            var retrieveOperation = TableOperation.Retrieve<MeterReadingEntity>(id, readingDateTime);

            // Execute the retrieve operation.
            var retrievedResult = meterTable.Execute(retrieveOperation);

            return (MeterReadingEntity) retrievedResult.Result;
        }

        public IQueryable<MeterReadingEntity> MeterReadings => meterTable.CreateQuery<MeterReadingEntity>();

        public IEnumerable<MeterReadingEntity> MeterReadingsFiltered(string finalFilter)
        {
            var query = new TableQuery<MeterReadingEntity>().Where(finalFilter);
            return meterTable.ExecuteQuery(query);
        }

        public long CountEntries()
        {
            var entities = meterTable.ExecuteQuery(new TableQuery()).ToList();
            return entities.Count();
        }
        /*
        public MeterReadingEntity MeterReadingsFiltered(string finalFilter, int resultCount)
        {
            var query = new TableQuery<MeterReadingEntity>().Where(finalFilter);
            return meterTable.ExecuteQuery(query).ToList().OrderByDescending(x => x.CreatedOn).FirstOrDefault();
        }
        */
    }
}
