using System.Diagnostics;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure.ServiceRuntime;
using System.Diagnostics.Tracing;

namespace AzureSBConnector
{
    [EventSource(Name = "StorageMonitorRoleSource")]
    sealed class EventSourceWriter : EventSource
    {
        public static EventSourceWriter Log = new EventSourceWriter();

        public void MessageMethod(string message) { if (IsEnabled())  WriteEvent(1, message); }
    }
    
    public class WorkerRole : RoleEntryPoint
    {
        // QueueClient is thread-safe. Recommended that you cache 
        // rather than recreating it on every request
      //  QueueClient Client;
        readonly ManualResetEvent _completedEvent = new ManualResetEvent(false);

        public override void Run()
        {
            Trace.WriteLine("Starting processing of messages");

            /*
            // Initiates the message pump and callback is invoked for each message that is received, calling close on the client will stop the pump.
            Client.OnMessage((receivedMessage) =>
                {
                    try
                    {
                        // Process the message
                        Trace.WriteLine("Processing Service Bus message: " + receivedMessage.SequenceNumber.ToString());
                    }
                    catch
                    {
                        // Handle any message processing specific exceptions here
                    }
                });
            */
            _completedEvent.WaitOne();
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections 
            ServicePointManager.DefaultConnectionLimit = 12;

     /*
            // Create the queue if it does not exist already
            string connectionString = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            if (!namespaceManager.QueueExists(QueueName))
            {
                namespaceManager.CreateQueue(QueueName);
            }

            // Initialize the connection to Service Bus Queue
            Client = QueueClient.CreateFromConnectionString(connectionString, QueueName);
      */
            return base.OnStart();
        }

        public override void OnStop()
        {
            // Close the connection to Service Bus Queue
       //     Client.Close();
            _completedEvent.Set();
            base.OnStop();
        }
    }
}
