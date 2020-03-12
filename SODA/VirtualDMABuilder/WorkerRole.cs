using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using DataAccess;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage.Table;

namespace VirtualDMABuilder
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        public override void Run()
        {
            Trace.TraceInformation("VirtualDMABuilder is running");

            try
            {
                RunAsync(cancellationTokenSource.Token).Wait();
            }
            finally
            {
                runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("VirtualDMABuilder has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("VirtualDMABuilder is stopping");

            cancellationTokenSource.Cancel();
            runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("VirtualDMABuilder has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
/*                        DateTime newDateTime = new DateTime(2015, 12, 09, 7, 0, 1);

                        while (newDateTime <= DateTime.Now)
                        {
                            runIngest(newDateTime);

                            newDateTime = newDateTime.AddHours(1);
                        }
  */            
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                await Task.Delay(300000);
                runIngest(DateTime.Now);
            }
        }

        private void runIngest(DateTime dateTimeNow)
        {
            var _currentContext = new SQLAzureDataContext();
            if (dateTimeNow.Minute < 5)
            {
                var dmaList = _currentContext.DMAs.Where(x => x.Virtual);
                foreach (var dma in dmaList)
                {
                    var thisContext = new MeterReadingTableStorageContext(dma.Site.TableName);

                    var meterList = _currentContext.Meters.Where(x => x.DMA.Identifier == dma.Identifier);

                    double flow = 0;
                    double volume = 0;

                    foreach (var meter in meterList)
                    {
                        var finalFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, meter.MeterIdentity);

                        var spanStart = new TimeSpan(0, 1, dateTimeNow.Minute, dateTimeNow.Second);
                        var spanEnd = new TimeSpan(0, 0, dateTimeNow.Minute, dateTimeNow.Second);

                        var spanBegin = dateTimeNow.Subtract(spanStart);
                        var date1 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, spanBegin.Ticks.ToString());
                        finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, date1);

                        var spanStop = dateTimeNow.Subtract(spanEnd);
                        var date2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, spanStop.Ticks.ToString());
                        finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, date2);

                        var allRecords = thisContext.MeterReadingsFiltered(finalFilter);

                        if (allRecords.Any())
                        {
                            var volumeSet = allRecords.Select(x => x.Reading);

                            var finalStartFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, meter.MeterIdentity);
                            var begindate = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, spanBegin.Ticks.ToString());
                            finalStartFilter = TableQuery.CombineFilters(finalStartFilter, TableOperators.And, begindate);

                            var startRecord =
                                thisContext.MeterReadingsFiltered(finalStartFilter)
                                    .OrderByDescending(x => x.CreatedOn)
                                    .First();

                            double startVolume;
                            DateTime startDate;
                            DateTime endDate = allRecords.Select(x => x.CreatedOn).Max();
                            if (startRecord != null)
                            {
                                startVolume = double.Parse(startRecord.Reading);
                                startDate = startRecord.CreatedOn;
                            }
                            else
                            {
                                startVolume = double.Parse(volumeSet.Min());
                                startDate = allRecords.Select(x => x.CreatedOn).Min();
                            }

                            var endVolume = double.Parse(volumeSet.Max());

                            double minutes = (endDate - startDate).Minutes;
                            if (minutes != 0)
                            {
                                minutes = minutes / 60;
                            }
                            double seconds = (endDate - startDate).Seconds;
                            if (seconds != 0)
                            {
                                seconds = seconds / 3600;
                            }

                            var hours = ((endDate - startDate).Days * 24) +
                                        (((endDate - startDate).Hours)) +
                                         minutes +
                                         seconds;

                            var multiplier = 1 / hours;

                            var hourlyVolume = (endVolume - startVolume) * multiplier;
                            volume = volume + (hourlyVolume);


                            flow = flow + (hourlyVolume / (hours * 3600));
                        }
                    }


                    var dmaData = new DMAData
                    {
                        Time = dateTimeNow.AddMilliseconds(dma.Id),
                        DMA = dma,
                        Flow = flow,
                        Volume = volume / 1000
                    };

                    _currentContext.DMADatas.InsertOnSubmit(dmaData);
                }
                _currentContext.SubmitChanges();
            }
        }
    }
}
