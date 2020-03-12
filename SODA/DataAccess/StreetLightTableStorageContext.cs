using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq;

namespace TableDataAccess
{
    public class StreetLightTableStorageContext
    {
        public static string StreetLightTableName = "StreetLightDataTable";
        CloudTable streetLightTable;

        public StreetLightTableStorageContext()
        {
            var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
            var tableClient = account.CreateCloudTableClient();
            streetLightTable = tableClient.GetTableReference(StreetLightTableName);
            streetLightTable.CreateIfNotExists();
        }

        public IQueryable<StreetLightEntity> StreetLightData => streetLightTable.CreateQuery<StreetLightEntity>();
    }
}
