using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.ServiceRuntime;
using System.Diagnostics.Tracing;

namespace BLOBStorageMonitor
{
    [EventSource(Name = "StorageMonitorRoleSource")]
    sealed class EventSourceWriter : EventSource
    {
        public static EventSourceWriter Log = new EventSourceWriter();

        public void MessageMethod(string message) { if (IsEnabled())  WriteEvent(1, message); }
    }

    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent _runCompleteEvent = new ManualResetEvent(false);

        public override void Run()
        {
            Trace.TraceInformation("RabbitMQConnector is running");

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

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            var result = base.OnStart();

            Trace.TraceInformation("RabbitMQConnector has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("RabbitMQConnector is stopping");

            _cancellationTokenSource.Cancel();
            _runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("RabbitMQConnector has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            var fileUploadMonitor = new FileUploadMonitor();

            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                await Task.Delay(10, cancellationToken);
                fileUploadMonitor.MonitorBlobStorage();
            }
        }
    }
}
