using DataAccess;
using Microsoft.Azure;
using System.Linq;

namespace TableDataAccess
{
    public class TopologyDataStorageContext
    {
        public static string TableName = "UrbanWaterTableStorage";
        public static string SmartMeterTableName = "MKWDNNetworkDataTable";

        #region Meteorological Data

        public IQueryable<MeteorologicalSensorEntity> MetSensorData
        {
            get
            {
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var meteorologicalSensorTable = tableClient.GetTableReference(TableName);
                meteorologicalSensorTable.CreateIfNotExists();

                return meteorologicalSensorTable.CreateQuery<MeteorologicalSensorEntity>();
            }
        }

        public IQueryable<MeteorologicalProductEntity> MetProductData
        {
            get
            {
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var meteorologicalProductTable = tableClient.GetTableReference(TableName);
                meteorologicalProductTable.CreateIfNotExists();

                return meteorologicalProductTable.CreateQuery<MeteorologicalProductEntity>();
            }
        }

        public IQueryable<MeteorologicalAssignmentEntity> MetAssignmentData
        {
            get
            {
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var meteorologicalAssignmentTable = tableClient.GetTableReference(TableName);
                meteorologicalAssignmentTable.CreateIfNotExists();

                return meteorologicalAssignmentTable.CreateQuery<MeteorologicalAssignmentEntity>();
            }
        }

        public IQueryable<RecordData> MetRecordData
        {
            get
            {
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var recordDataTable = tableClient.GetTableReference(TableName);
                recordDataTable.CreateIfNotExists();

                return recordDataTable.CreateQuery<RecordData>();
            }
        }

        public IQueryable<RasterFields> MetRasterFieldData
        {
            get
            {
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var rasterFieldsTable = tableClient.GetTableReference(TableName);
                rasterFieldsTable.CreateIfNotExists();

                return rasterFieldsTable.CreateQuery<RasterFields>();
            }
        }

        public IQueryable<RasterRecordData> MetRasterRecordData
        {
            get
            {
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var rasterRecordTable = tableClient.GetTableReference(TableName);
                rasterRecordTable.CreateIfNotExists();

                return rasterRecordTable.CreateQuery<RasterRecordData>();
            }
        }

        #endregion

        #region Meter Data

        public IQueryable<MeterReadingEntity> MeterReadings
        {
            get
            {
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var meterReadingTable = tableClient.GetTableReference(TableName);
                meterReadingTable.CreateIfNotExists();

                return meterReadingTable.CreateQuery<MeterReadingEntity>();
            }
        }

        #endregion

        #region Topology Data
        //Toplogy Data
        public IQueryable<TopLineEntity> TopologyLine
        {
            get
            {
                //Multiple tables foreach
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var topLineTable = tableClient.GetTableReference(TableName);
                topLineTable.CreateIfNotExists();

                return topLineTable.CreateQuery<TopLineEntity>();
            }
        }

        public IQueryable<TopNodeEntity> TopologyNode
        {
            get
            {
                //Multiple tables foreach
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var topNodeTable = tableClient.GetTableReference(TableName);
                topNodeTable.CreateIfNotExists();

                return topNodeTable.CreateQuery<TopNodeEntity>();
            }
        }

        public IQueryable<TopReservoirEntity> TopologyReservoir
        {
            get
            {
                //Multiple tables foreach
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var topReservoirTable = tableClient.GetTableReference(TableName);
                topReservoirTable.CreateIfNotExists();

                return topReservoirTable.CreateQuery<TopReservoirEntity>();
            }
        }

        public IQueryable<TopTankEntity> TopologyTank
        {
            get
            {
                //Multiple tables foreach
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var topTankTable = tableClient.GetTableReference(TableName);
                topTankTable.CreateIfNotExists();

                return topTankTable.CreateQuery<TopTankEntity>();
            }
        }

        public IQueryable<TopJunctionsEntity> TopologyJunctions
        {
            get
            {
                //Multiple tables foreach
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var topJunctionsTable = tableClient.GetTableReference(TableName);
                topJunctionsTable.CreateIfNotExists();

                return topJunctionsTable.CreateQuery<TopJunctionsEntity>();
            }
        }

        public IQueryable<TopetypeOfValve> TopValveType
        {
            get
            {
                //Multiple tables foreach
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var topetypeOfValveTable = tableClient.GetTableReference(TableName);
                topetypeOfValveTable.CreateIfNotExists();

                return topetypeOfValveTable.CreateQuery<TopetypeOfValve>();
            }
        }

        #endregion

        #region Calendar
        //Calender Data
        public IQueryable<CalendarEntity> CalendarData
        {
            get
            {
                //Multiple tables foreach
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("DataConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var calendarTable = tableClient.GetTableReference(TableName);
                calendarTable.CreateIfNotExists();

                return calendarTable.CreateQuery<CalendarEntity>();
            }
        }
        #endregion
    }
}
