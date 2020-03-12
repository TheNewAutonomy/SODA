using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace ServiceBusMonitor.Processor
{
    public class StreetLightQueueProcessor : QueueProcessorBase, IQueueProcessor
    {
        public void ProcessQueue(CloudBlockBlob blob,
                                 CloudQueueMessage receivedMessage,
                                 CloudQueue urbanWaterQueue)
        {

        }
    }
}
