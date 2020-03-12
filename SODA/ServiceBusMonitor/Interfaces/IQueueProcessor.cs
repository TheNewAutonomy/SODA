using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace ServiceBusMonitor
{
    interface IQueueProcessor
    {
        void ProcessQueue(CloudBlockBlob blob,
                          CloudQueueMessage receivedMessage,
                          CloudQueue urbanWaterQueue);
    }
}
