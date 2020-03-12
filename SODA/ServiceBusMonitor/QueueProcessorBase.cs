using DataAccess;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace ServiceBusMonitor
{
    public abstract class QueueProcessorBase
    {
        public enum EventTypes { NewMeteringData = 1, ServiceStart = 2, MissingMeter = 3 };
        public SQLAzureDataContext CurrentContext;

        protected QueueProcessorBase()
        {
            CurrentContext = new SQLAzureDataContext();
        }

        public static bool WriteMessageMeterDataToDataTable(MeterReadingEntity sm, string strConnectionString, string strStorageTableName)
        {
            try
            {
                var storageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting(strConnectionString));

                // Create the table client. 
                var tableClient = storageAccount.CreateCloudTableClient();
                
                var storageTable = tableClient.GetTableReference(strStorageTableName);
                storageTable.CreateIfNotExists();
                
                try
                {
                    if (sm != null)
                    {
                        var insertRecord = TableOperation.Insert(sm);
                        storageTable.Execute(insertRecord);
                    }
                    return true;
                }
                catch
                {
                    return false;
                }
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
