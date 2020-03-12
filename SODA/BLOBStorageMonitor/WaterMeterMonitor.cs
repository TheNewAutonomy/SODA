using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BLOBStorageMonitor
{
    public class WaterMeterMonitor
    {
        public void MonitorBlobStorage(string strType, string strConnectionString, string strTableName, List<string> containers)
        {
            try
            {
                // Get account details
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting(strConnectionString));
            
                // Create the blob and table client. 
                CloudBlobClient blobClient   = account.CreateCloudBlobClient();
                CloudTableClient tableClient = account.CreateCloudTableClient();

                // Create the table if it doesn't exist
                CloudTable table = tableClient.GetTableReference(strTableName);
                table.CreateIfNotExists();

                // Loop through each container, usually one for each utility
                foreach (string strContainer in containers)
                {
                    // Retrieve reference to a previously created container.
                    CloudBlobContainer container = blobClient.GetContainerReference(strContainer);

                    // Loop over items (files) within the container and output the length and URI.
                    foreach (IListBlobItem item in container.ListBlobs())
                    {
                        if (item.GetType() == typeof(CloudBlockBlob))
                        {
                            CloudBlockBlob blob = (CloudBlockBlob)item;

                            string strName = blob.Uri.ToString();
                            strName = Path.GetFileName(strName);

                            // Retrieve the entity with partition key of "Smith" and row key of "Jeff" 
                            TableOperation fileNameQuery = TableOperation.Retrieve<FileInventoryEntity>(strContainer, strName);

                            // Retrieve entity 
                            FileInventoryEntity fileNameEntity = (FileInventoryEntity)table.Execute(fileNameQuery).Result;

                            // If the current file doesn't exist, add an entry
                            if (fileNameEntity == null)
                            {
                                //if a new file, make entity and add to table
                                FileInventoryEntity inventoryEntity = new FileInventoryEntity();
                                inventoryEntity.PartitionKey        = strContainer;
                                inventoryEntity.RowKey              = strName;
                                inventoryEntity.lngFileLength       = blob.Properties.Length;
                                inventoryEntity.Etag                = blob.Properties.ETag;
                                inventoryEntity.UploadDateTime      = DateTime.Now;

                                bool test = StorageMonitorUtility.WriteFileDataToInventoryDataTable(inventoryEntity, strConnectionString, strTableName);

                                //call recursive etag check on file to check is it uploaded
                                StorageMonitorUtility.CheckETagOfAddedFile(inventoryEntity, strConnectionString, strType);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                EventSourceWriter.Log.MessageMethod("Exception in storage moitoring MonitorBlobStorage" + ex.Message.ToString());
            }
        }
    }
}
