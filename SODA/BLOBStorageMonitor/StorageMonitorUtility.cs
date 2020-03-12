using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.IO;
using System.Linq;
using System.Threading;

namespace BLOBStorageMonitor
{
    public class StorageMonitorUtility
    {
        public static bool WriteFileDataToInventoryDataTable(FileInventoryEntity fi)
        {
            try
            {
                EventSourceWriter.Log.MessageMethod(
                    $"Adding Message to Queue for File: {fi.RowKey}, Container: {fi.PartitionKey}");

                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
                var tableClient = account.CreateCloudTableClient();
                var table = tableClient.GetTableReference(CloudConfigurationManager.GetSetting("FileInventory"));
                table.CreateIfNotExists();

                try
                {
                    // Create an operation to add the new customer to the people table. 
                    var insertFileEntry = TableOperation.Insert(fi);

                    // Submit the operation to the table service. 
                    table.Execute(insertFileEntry);
                    return true;
                }
                catch
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                EventSourceWriter.Log.MessageMethod(
                    $"Exception in Storage Monitor WriteFileDataToInventoryDataTable{ex.Message}");
                return false;
            }
        }

        public static void CheckETagOfAddedFile(FileInventoryEntity entity)
        {
            try
            {
                EventSourceWriter.Log.MessageMethod($"Checking Etag of added file {entity.RowKey}");
                var blCondition = false;
                var etag = entity.Etag;

                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
            
                // Create the blob client. 
                var blobClient = account.CreateCloudBlobClient();

                // Retrieve reference to a previously created container.
                var container = blobClient.GetContainerReference(entity.PartitionKey);

                do
                {
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

                            foreach (IListBlobItem item in outerDirectoryBlobs)
                            {
                                if (item is CloudBlockBlob)
                                {
                                    var blob = (CloudBlockBlob)item;

                                    var strName = blob.Uri.ToString();
                                    strName = Path.GetFileName(strName);

                                    if (strName == entity.RowKey)
                                    {
                                        if (blob.Properties.ETag == etag)
                                        {
                                            AddmessagetoQueue(entity, "StorageConnectionString", entity.PartitionKey);
                                            blCondition = true;
                                            break;
                                        }

                                        else
                                        {
                                            etag = entity.Etag;
                                            Thread.Sleep(1000);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else if (blobs.Count() > 0)
                    {
                        foreach (IListBlobItem item in blobs)
                        {
                            if (item is CloudBlockBlob)
                            {
                                var blob = (CloudBlockBlob)item;

                                var strName = blob.Uri.ToString();
                                strName = Path.GetFileName(strName);

                                if (strName == entity.RowKey)
                                {
                                    if (blob.Properties.ETag == etag)
                                    {
                                        AddmessagetoQueue(entity, "StorageConnectionString", entity.PartitionKey);
                                        blCondition = true;
                                        break;
                                    }

                                    else
                                    {
                                        etag = entity.Etag;
                                        Thread.Sleep(1000);
                                    }
                                }
                            }
                        }
                    }
                } while (!blCondition);
            }
            catch (Exception ex)
            {
                EventSourceWriter.Log.MessageMethod($"Exception in CheckETagOfAddedFile {ex.Message}");
            }
        }

        public static void AddmessagetoQueue(FileInventoryEntity entity, string strConnectionString, string strType)
        {
            try
            {
                EventSourceWriter.Log.MessageMethod($"Adding Message to Queue for file {entity.RowKey}");

                // Retrieve storage account from connection string
                var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

                // Create the queue client
                var queueClient = account.CreateCloudQueueClient();

                // Retrieve a reference to a queue
                var queue = queueClient.GetQueueReference("urbanwaterqueue");

                // Create the queue if it doesn't already exist
                queue.CreateIfNotExists();

                // Create the queue message. 
                //Meesage must be in the order type, filename, container, size, connection string
                var queueMessageString =
                    entity.RowKey + "," +
                    entity.PartitionKey + "," +
                    entity.LngFileLength + "," +
                    strConnectionString;

                var queueMessage = new Microsoft.WindowsAzure.Storage.Queue.CloudQueueMessage(queueMessageString);
                queue.AddMessage(queueMessage);
            }
            catch (Exception ex)
            {
                EventSourceWriter.Log.MessageMethod("Exception in AddmessagetoQueue" + ex.Message);
            }
        }
    }
}
