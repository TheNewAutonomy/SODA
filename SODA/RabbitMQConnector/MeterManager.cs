using DataAccess;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static System.Int32;

namespace RabbitMQConnector
{
    public class MeterManager
    {
        readonly RequestManager _currentRequestManager;
        SQLAzureDataContext _currentContext;

        public MeterManager(RequestManager requestManager)
        {
            _currentRequestManager = requestManager;
        }

        public static long CountEntries()
        {
            var thisContext = new MeterReadingTableStorageContext("aqualiameterdata");
            return thisContext.CountEntries();
        }
        public string ReadList()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.First(kvp => kvp.Key == "elementId").Value;
            var meters = _currentContext.GetmeterList(elementId);
            var resultTxt = meters.Aggregate(string.Empty, (current, thisReading) => current + ("<record>" + "<variable name=\"meterId\">" + $"<value>{thisReading.MeterIdentity}</value>" + "</variable>" + $"</record>{Environment.NewLine}"));

            var response = "<response>" + "<recordSet>" +
                              $"<elementId>{elementId}</elementId>{resultTxt}</recordSet>" + "</response>";

            return response;
        }
        
        public string Read(bool bWrap)
        {
            _currentContext = new SQLAzureDataContext();

            var meterId = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "elementId") != 0 ?
                            _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value : string.Empty;
            var start = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "start") ?
                            DateTime.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value) : DateTime.MinValue;
            var end = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "end") ?
                            DateTime.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value) : DateTime.MaxValue;
            long timestep = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "timeStep") ?
                            Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;

            var resultTxt = new StringBuilder();
            var elementText = "<elementId>" + meterId + "</elementId>";

            var site = _currentContext.Meters.FirstOrDefault(x => x.MeterIdentity == meterId)?.DMA.Site;

            if (!string.IsNullOrEmpty(meterId) && site != null)
            {
                var thisContext = new MeterReadingTableStorageContext(site.TableName);

                if (timestep == -1)
                {
                    var finalFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, meterId);

                    if (start != DateTime.MinValue)
                    {
                        var date1 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, start.Ticks.ToString());
                        finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, date1);
                    }

                    if (end != DateTime.MaxValue)
                    {
                        var date2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, end.Ticks.ToString());
                        finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, date2);
                    }

                    var allRecords = thisContext.MeterReadingsFiltered(finalFilter);

                    foreach (var record in allRecords)
                    {
                        resultTxt.Append("<record>" + $"<time>{record.CreatedOn.ToString("yyyy-MM-ddTHH:mm:sszzz")}</time><variable name=\"flow\">" + $"<value>{record.Reading}</value></variable>" + $"</record>{Environment.NewLine}");
                    }
                }
                else
                {
                    const long multiplier = 10000000;
                    long timestepTicks = timestep * multiplier;
                    long numberOfTicksBetweenStartAndEnd = (long)(end.Ticks - start.Ticks);
                    int numberOfBlock = (int)(numberOfTicksBetweenStartAndEnd / timestepTicks);

                    // Create a collection of fixed blocks in time from supplied Start to End
                    var dateTimeBlockSet = new List<DateTimeBlock>();
                    for (var blockIndex = 0; blockIndex < numberOfBlock; blockIndex++)
                    {
                        var newDateTimeBlock = new DateTimeBlock
                        {
                            From = start.AddTicks(blockIndex * timestepTicks),
                            To = start.AddTicks((blockIndex + 1) * timestepTicks)
                        };

                        dateTimeBlockSet.Add(newDateTimeBlock);
                    }

                    // loop through each block and and find any records that span that block, trimming those on the edges
                    foreach (var thisBlock in dateTimeBlockSet)
                    {
                        var recordsSpanningBlock = new List<MeterReadingEntity>();

                        var finalFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, meterId);

                        if (start != DateTime.MinValue)
                        {
                            var date1 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, thisBlock.From.Ticks.ToString());
                            finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, date1);
                        }

                        if (end != DateTime.MaxValue)
                        {
                            var date2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, thisBlock.To.Ticks.ToString());
                            finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, date2);
                        }

                        recordsSpanningBlock.AddRange(thisContext.MeterReadingsFiltered(finalFilter));

                        if (recordsSpanningBlock.Any())
                        {
                            //    var minResult = recordsSpanningBlock.Min(x => x.Reading);
                            //    var maxResult = recordsSpanningBlock.Max(x => x.Reading);
                            var minResult = float.Parse(recordsSpanningBlock.Min(x => x.Reading));
                            var maxResult = float.Parse(recordsSpanningBlock.Max(x => x.Reading));

                            var averageValue = (minResult + maxResult) / 2;

                            //                        if (!string.IsNullOrEmpty(minResult) && !string.IsNullOrEmpty(maxResult))
                            {
                                //                          var flow = Int32.Parse(maxResult) - Int32.Parse(minResult);
                                resultTxt.Append("<record>" +
                                             $"<time>{thisBlock.From.ToString("yyyy-MM-ddTHH:mm:sszzz")}</time><variable name=\"flow\">" + $"<value>{averageValue}</value></variable>" + $"</record>{Environment.NewLine}");
                            }
                        }
                    }
                }
            }

            if (bWrap)
            {
                return "<response>" + $"<recordSet>{elementText}{resultTxt}</recordSet>" + "</response>";

            }
            else
            {
                return resultTxt.ToString();
            }
        }

        public string DMARead(bool bWrap)
        {
            _currentContext = new SQLAzureDataContext();

            var dma = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "elementId") != 0 ?
                            _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value : string.Empty;
            var start = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "start") ?
                            DateTime.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value) : DateTime.MinValue;
            var end = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "end") ?
                            DateTime.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value) : DateTime.MaxValue;
            long timestep = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "timeStep") ?
                            Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;

            var resultTxt = new StringBuilder();
            var elementText = $"<elementId>{dma}</elementId>";

            var site = _currentContext.DMAs.FirstOrDefault(x => x.Identifier == dma)?.Site;

            if (!string.IsNullOrEmpty(dma) && site != null)
            {
                var thisContext = new DMAMeterReadingTableStorageContext(site.TableName);

                if (timestep == -1)
                {
                    var meterList = _currentContext.Meters.Where(x => x.DMA.Identifier == dma);

                    foreach (var meter in meterList)
                    {
                        var finalFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, meter.MeterIdentity);

                        if (start != DateTime.MinValue)
                        {
                            var date1 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual,
                                start.Ticks.ToString());
                            finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, date1);
                        }

                        if (end != DateTime.MaxValue)
                        {
                            var date2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                                end.Ticks.ToString());
                            finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, date2);
                        }

                        var allRecords = thisContext.MeterReadingsFiltered(finalFilter);

                        foreach (var record in allRecords)
                        {
                            resultTxt.Append("<record>" + $"<meterId>{record.MeterID}</meterId>" +
                                             $"<time>{record.CreatedOn.ToString("yyyy-MM-ddTHH:mm:sszzz")}</time><variable name=\"flow\">" +
                                             $"<value>{record.Reading}</value></variable>" +
                                             $"</record>{Environment.NewLine}");
                        }
                    }
                }
                else
                {
                    const long multiplier = 10000000;
                    long timestepTicks = timestep * multiplier;
                    long numberOfTicksBetweenStartAndEnd = (long)(end.Ticks - start.Ticks);
                    int numberOfBlock = (int)(numberOfTicksBetweenStartAndEnd / timestepTicks);

                    // Create a collection of fixed blocks in time from supplied Start to End
                    var dateTimeBlockSet = new List<DateTimeBlock>();
                    for (var blockIndex = 0; blockIndex < numberOfBlock; blockIndex++)
                    {
                        var newDateTimeBlock = new DateTimeBlock
                        {
                            From = start.AddTicks(blockIndex * timestepTicks),
                            To = start.AddTicks((blockIndex + 1) * timestepTicks)
                        };

                        dateTimeBlockSet.Add(newDateTimeBlock);
                    }

                    // loop through each block and and find any records that span that block, trimming those on the edges
                    foreach (var thisBlock in dateTimeBlockSet)
                    {
                        var recordsSpanningBlock = new List<DMAMeterReadingEntity>();

                        var meterList = _currentContext.Meters.Where(x => x.DMA.Identifier == dma);
                        foreach (var meter in meterList)
                        {
                            var finalFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal,
                                dma);

                            if (start != DateTime.MinValue)
                            {
                                var date1 = TableQuery.GenerateFilterCondition("RowKey",
                                    QueryComparisons.GreaterThanOrEqual,
                                    thisBlock.From.Ticks.ToString());
                                finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, date1);
                            }

                            if (end != DateTime.MaxValue)
                            {
                                var date2 = TableQuery.GenerateFilterCondition("RowKey",
                                    QueryComparisons.LessThanOrEqual,
                                    thisBlock.To.Ticks.ToString());
                                finalFilter = TableQuery.CombineFilters(finalFilter, TableOperators.And, date2);
                            }

                            recordsSpanningBlock.AddRange(thisContext.MeterReadingsFiltered(finalFilter));

                            if (recordsSpanningBlock.Any())
                            {
                                var minResult = Int32.Parse(recordsSpanningBlock.Min(x => x.Reading));
                                var maxResult = Int32.Parse(recordsSpanningBlock.Max(x => x.Reading));

                                var averageValue = (minResult + maxResult) / 2;

                                //     if (!string.IsNullOrEmpty(minResult) && !string.IsNullOrEmpty(maxResult))
                                //     {

                                //   var flow = Int32.Parse(maxResult) - Int32.Parse(minResult);
                                resultTxt.Append("<record>" +
                                                 $"<meterId>{meter.MeterIdentity}</meterId><variable name=\"flow\">" +
                                                 $"<value>{averageValue}</value></variable>" +
                                                 $"</record>{Environment.NewLine}");

                                //     }
                            }
                        }
                    }
                }
            }

            if (bWrap)
            {
                return "<response>" + $"<recordSet>{elementText}{resultTxt}</recordSet>" + "</response>";

            }
            else
            {
                return resultTxt.ToString();
            }
        }
    }
}
