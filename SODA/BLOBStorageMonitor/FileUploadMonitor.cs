using DataAccess;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.IO;
using System.Linq;

namespace BLOBStorageMonitor
{
    public class FileUploadMonitor
    {
        public void MonitorBlobStorage()
        {
            try
            {
                // Get account details
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
            
                // Create the blob and table client. 
                var blobClient   = account.CreateCloudBlobClient();
                var tableClient = account.CreateCloudTableClient();

                // Create the file logging table if it doesn't exist
                var table = tableClient.GetTableReference(CloudConfigurationManager.GetSetting("FileInventory"));
                table.CreateIfNotExists();

                // Get a list of all the active containers
                var currentContext = new SQLAzureDataContext();
                var containers = currentContext.Containers.Where(x => x.Active).Select(x => x.FileContainers).ToList();
            
                // Loop through each container, usually one for each utility
                foreach (var strContainer in containers)
                {
                    // Retrieve reference to a previously created container.
                    var container = blobClient.GetContainerReference(strContainer);
                    container.CreateIfNotExists();

                    var blobs = container.ListBlobs();

                    if (blobs.Count() > 0 && blobs.FirstOrDefault().GetType() == typeof(CloudBlobDirectory))
                    {
                        foreach (CloudBlobDirectory outerdirectory in blobs)
                        {
                            var outerDirectoryBlobs = outerdirectory.ListBlobs();

                            while (outerDirectoryBlobs.Any() && outerDirectoryBlobs.FirstOrDefault().GetType() == typeof(CloudBlobDirectory))
                            {
                                var directory = (CloudBlobDirectory)outerDirectoryBlobs.FirstOrDefault();
                                outerDirectoryBlobs = directory.ListBlobs();
                            }

                            // Loop over items (files) within the container and output the length and URI.
                            foreach (var item in outerDirectoryBlobs)
                            {
                                if (item is CloudBlockBlob)
                                {
                                    var blob = (CloudBlockBlob)item;

                                    var strName = blob.Uri.ToString();
                                    strName = Path.GetFileName(strName);

                                    var fileNameQuery = TableOperation.Retrieve<FileInventoryEntity>(strContainer, strName);

                                    // Retrieve entity 
                                    var fileNameEntity = (FileInventoryEntity)table.Execute(fileNameQuery).Result;


                                    // If the current file doesn't exist, add an entry
                                    if (fileNameEntity == null)
                                    {
                                        //if a new file, make entity and add to table
                                        var inventoryEntity = new FileInventoryEntity
                                        {
                                            PartitionKey = strContainer,
                                            RowKey = strName,
                                            LngFileLength = blob.Properties.Length,
                                            Etag = blob.Properties.ETag,
                                            UploadDateTime = DateTime.Now
                                        };

                                        StorageMonitorUtility.WriteFileDataToInventoryDataTable(inventoryEntity);

                                        //call recursive etag check on file to check is it uploaded
                                        StorageMonitorUtility.CheckETagOfAddedFile(inventoryEntity);
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        foreach (var item in blobs)
                        {
                            if (item is CloudBlockBlob)
                            {
                                var blob = (CloudBlockBlob)item;

                                var strName = blob.Uri.ToString();
                                strName = Path.GetFileName(strName);

                                var fileNameQuery = TableOperation.Retrieve<FileInventoryEntity>(strContainer, strName);

                                // Retrieve entity 
                                var fileNameEntity = (FileInventoryEntity)table.Execute(fileNameQuery).Result;


                                // If the current file doesn't exist, add an entry
                                if (fileNameEntity == null)
                                {
                                    //if a new file, make entity and add to table
                                    var inventoryEntity = new FileInventoryEntity
                                    {
                                        PartitionKey = strContainer,
                                        RowKey = strName,
                                        LngFileLength = blob.Properties.Length,
                                        Etag = blob.Properties.ETag,
                                        UploadDateTime = DateTime.Now
                                    };

                                    StorageMonitorUtility.WriteFileDataToInventoryDataTable(inventoryEntity);

                                    //call recursive etag check on file to check is it uploaded
                                    StorageMonitorUtility.CheckETagOfAddedFile(inventoryEntity);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                EventSourceWriter.Log.MessageMethod($"Exception in storage moitoring MonitorBlobStorage{ex.Message}");
            }
        }
    }
}
