using DataAccess;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceBusMonitor
{
    interface IQueueProcessor
    {
        void ProcessQueue(CloudBlockBlob blob,
                          string strContainer,
                          CloudQueueMessage receivedMessage,
                          CloudQueue urbanWaterQueue);
    }
}
