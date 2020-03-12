using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq;

namespace TableDataAccess
{
    public class CalendarTableStorageContext
    {
        public static string TableName = "UrbanWaterTableStorage";
        CloudTable calendarTable;

        public CalendarTableStorageContext()
        {
            var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
            var tableClient = account.CreateCloudTableClient();
            calendarTable = tableClient.GetTableReference(TableName);
            calendarTable.CreateIfNotExists();
        }

        public IQueryable<CalendarEntity> CalenderData => calendarTable.CreateQuery<CalendarEntity>();
    }
}
