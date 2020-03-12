using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure.ServiceRuntime;
using System.Diagnostics.Tracing;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure;
using ServiceBusMonitor.Processor;

namespace ServiceBusMonitor
{
    [EventSource(Name = "QueueWorkerRoleSource")]
    sealed class EventSourceWriter : EventSource
    {
        public static readonly EventSourceWriter Log = new EventSourceWriter();

        public void MessageMethod(string message) { if (IsEnabled())  WriteEvent(1, message); }
    }

    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent _runCompleteEvent = new ManualResetEvent(false);

        CloudQueue _urbanWaterQueue;
        CloudQueueMessage _receivedMessage;

        public override void Run()
        {
            Trace.TraceInformation("QueueWorkerRole is running");

            try
            {
                RunAsync(_cancellationTokenSource.Token).Wait();
            }
            finally
            {
                _runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the queue client
            var queueClient =
                account.CreateCloudQueueClient();

            // Retrieve a reference to a queue
            _urbanWaterQueue = queueClient.GetQueueReference("urbanwaterqueue");

            // Create the queue if it doesn't already exist
            _urbanWaterQueue.CreateIfNotExists();

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            var result = base.OnStart();

            Trace.TraceInformation("QueueWorkerRole has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("QueueWorkerRole is stopping");

            _cancellationTokenSource.Cancel();
            _runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("QueueWorkerRole has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            IQueueProcessor queueProcessor = null;

            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                await Task.Delay(10, cancellationToken);

                try
                {
                    // Retrieve and process a new message from the queue.
                    _receivedMessage = _urbanWaterQueue.GetMessage();

                    if (_receivedMessage != null)
                    {
                        if (_receivedMessage.DequeueCount > 5)
                        {
                            EventSourceWriter.Log.MessageMethod("Worker Role Processing dequeued message after 5 failure Attempts " + _receivedMessage.Id);
                            _urbanWaterQueue.DeleteMessage(_receivedMessage);
                            return;
                        }
                        EventSourceWriter.Log.MessageMethod("Worker Role Processing recived message from Queue " + _receivedMessage.Id);

                        var messageParts = _receivedMessage.AsString.Split(',');
                        var strType = messageParts[1];
                        var strFileName = messageParts[0];
                        var strContainer = messageParts[1];
                        var strConnectionString = messageParts[3];

                        // Retrieve storage account from connection string.
                        var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting(strConnectionString));

                        // Create the blob client.
                        var blobClient = account.CreateCloudBlobClient();

                        // Retrieve reference to a previously created container.
                        var container = blobClient.GetContainerReference(strContainer);

                        // Retrieve reference to a blob file.
                        var blockBlob = container.GetBlockBlobReference(strFileName);


                        var blobs = container.ListBlobs();
                        while (blobs.Any() && blobs.FirstOrDefault() is CloudBlobDirectory)
                        {
                            var directory = (CloudBlobDirectory)blobs.FirstOrDefault();
                            blobs = directory.ListBlobs();

                            if (blobs.FirstOrDefault().GetType() == typeof(CloudBlockBlob))
                            {
                                blockBlob = directory.GetBlockBlobReference(strFileName);
                            }
                        }

                        switch (strType)
                        {
                            case "sw-sagem":
                                {
                                    queueProcessor = new WaterMeterQueueXMLProcessor();
                                    break;
                                }
                            case "aqualiameterdata":
                                {
                                    queueProcessor = new WaterMeterCSVProcessor_Aqualia();
                                    break;
                                }
                            case "ovodmeterdata":
                                {
                                    queueProcessor = new WaterMeterExcelProcessor_Ovod();
                                    break;
                                }
                            case "Weatherdata":
                                {
                                    queueProcessor = new WeatherQueueProcessor();
                                    break;
                                }
                        }

                        queueProcessor?.ProcessQueue(blockBlob, _receivedMessage, _urbanWaterQueue);
                    }
                }
                catch (Exception e)
                {
                    EventSourceWriter.Log.MessageMethod("Exception thrown in processor Worker role " + e.Message);
                }
            }
        }
    }
}
