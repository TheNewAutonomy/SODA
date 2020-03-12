using DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RabbitMQConnector
{
    public class DMADataManager
    {
        readonly RequestManager _currentRequestManager;
        SQLAzureDataContext _currentContext;

        public DMADataManager(RequestManager requestManager)
        {
            _currentRequestManager = requestManager;
        }

        public string ReadSpecificProperty(List<int> indices)
        {
            _currentContext  = new SQLAzureDataContext();
            var elementId    = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start        = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "start") ?
                               DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value) : DateTimeOffset.MinValue;
            var end          = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "end") ?
                               DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value) : DateTimeOffset.MaxValue;
            long timestep    = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "timeStep") > 0 ?
                               int.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;

            var resultTxt = new StringBuilder();
            var allResults   = _currentContext.GetDMAData(start, end, elementId).ToList();

            if (allResults.Any())
            {
                if (timestep != -1 && allResults.Any())
                {
                    const long multiplier = 10000000;
                    long timestepTicks = timestep*multiplier;
                    long numberOfTicksBetweenStartAndEnd = (long) (end.Ticks - start.Ticks);
                    int numberOfBlock = (int) (numberOfTicksBetweenStartAndEnd/timestepTicks);

                    // Create a collection of fixed blocks in time from supplied Start to End
                    var dateTimeBlockSet = new List<DateTimeBlock>();
                    for (var blockIndex = 0; blockIndex < numberOfBlock; blockIndex++)
                    {
                        var newDateTimeBlock = new DateTimeBlock
                        {
                            From = start.AddTicks(blockIndex*timestepTicks),
                            To = start.AddTicks((blockIndex + 1)*timestepTicks)
                        };

                        dateTimeBlockSet.Add(newDateTimeBlock);
                    }

                    var subResults = new List<GetDMADataResult>();

                    // loop through each block and and find any records that span that block, trimming those on the edges
                    foreach (var thisBlock in dateTimeBlockSet)
                    {
                        var recordsSpanningBlock =
                            allResults.Where(x => x.Time > thisBlock.From && x.Time <= thisBlock.To);

                        var getDmaDataResults = recordsSpanningBlock as GetDMADataResult[] ??
                                                recordsSpanningBlock.ToArray();
                        if (getDmaDataResults.Any())
                        {
                            double? flow = null;
                            double? pressure = null;
                            double? Volume = null;

                            foreach (var reading in getDmaDataResults)
                            {
                                if (reading.Flow != null)
                                {
                                    if (flow == null)
                                    {
                                        flow = reading.Flow;
                                    }
                                    else
                                    {
                                        flow += reading.Flow;
                                    }
                                }

                                if (reading.Pressure != null)
                                {
                                    if (pressure == null)
                                    {
                                        pressure = reading.Pressure;
                                    }
                                    else
                                    {
                                        pressure += reading.Pressure;
                                    }
                                }

                                if (reading.Volume != null)
                                {
                                    if (Volume == null)
                                    {
                                        Volume = reading.Volume;
                                    }
                                    else
                                    {
                                        Volume += reading.Volume;
                                    }
                                }
                            }

                            var modifiedReading = getDmaDataResults.FirstOrDefault();

                            if (flow != null)
                                if (modifiedReading != null) modifiedReading.Flow = flow;
                            if (pressure != null)
                                if (modifiedReading != null)
                                    modifiedReading.Pressure = pressure/getDmaDataResults.Count(x => x.Pressure != null);
                            if (Volume != null)
                                if (modifiedReading != null) modifiedReading.Volume = Volume;

                            if (modifiedReading != null)
                            {
                                modifiedReading.Time = thisBlock.From;

                                subResults.Add(modifiedReading);
                            }
                        }
                    }

                    allResults = subResults;
                }

                foreach (var thisReading in allResults)
                {
                    var flowValue = string.Empty;
                    var pressureValue = string.Empty;
                    var volumeValue = string.Empty;

                    if (indices.Contains(16) && thisReading.Flow != null)
                    {
                        flowValue = "<variable name=\"flow\">" + $"<value>{thisReading.Flow}</value>" + "</variable>";
                    }
                    if (indices.Contains(17) && thisReading.Pressure != null)
                    {
                        pressureValue = "<variable name=\"pressure\">" + $"<value>{thisReading.Pressure}</value>" +
                                        "</variable>";
                    }
                    if (indices.Contains(18) && thisReading.Volume != null)
                    {
                        volumeValue = "<variable name=\"volume\">" + $"<value>{thisReading.Volume}</value>" +
                                      "</variable>";
                    }

                    if (!string.IsNullOrEmpty(flowValue) || !string.IsNullOrEmpty(pressureValue) ||
                        !string.IsNullOrEmpty(volumeValue))
                    {
                        resultTxt.Append($"<record><time>{thisReading.Time.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</time>{flowValue}{pressureValue}{volumeValue}</record>{Environment.NewLine}");
                    }
                }
            }

            return resultTxt.ToString();
        }

        public string Read()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start     = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value);
            var end       = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value);
            long timestep = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "timeStep") > 0 ?
                            int.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;
            
            var allResults = _currentContext.GetDMAData(start, end, elementId).ToList();

            if (timestep != -1 && allResults.Any())
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
                        From = start.AddTicks(blockIndex*timestepTicks),
                        To = start.AddTicks((blockIndex + 1)*timestepTicks)
                    };

                    dateTimeBlockSet.Add(newDateTimeBlock);
                }

                var subResults = new List<GetDMADataResult>();

                // loop through each block and and find any records that span that block, trimming those on the edges
                foreach (var thisBlock in dateTimeBlockSet)
                {
                    var recordsSpanningBlock = allResults.Where(x => x.Time > thisBlock.From && x.Time <= thisBlock.To);

                    var getDmaDataResults = recordsSpanningBlock as GetDMADataResult[] ?? recordsSpanningBlock.ToArray();
                    if (getDmaDataResults.Any())
                    {
                        double? flow = null;
                        double? pressure = null;
                        double? volume = null;

                        foreach (var reading in getDmaDataResults)
                        {
                            if (reading.Flow != null)
                            {
                                if (flow == null)
                                {
                                    flow  = reading.Flow;
                                }
                                else
                                {
                                    flow += reading.Flow;
                                }
                            }

                            if (reading.Pressure != null)
                            {
                                if (pressure == null)
                                {
                                    pressure = reading.Pressure;
                                }
                                else
                                {
                                    pressure += reading.Pressure;
                                }
                            }

                            if (reading.Volume != null)
                            {
                                if (volume == null)
                                {
                                    volume = reading.Volume;
                                }
                                else
                                {
                                    volume += reading.Volume;
                                }
                            }
                        }

                        var modifiedReading = getDmaDataResults.FirstOrDefault();

                        if (flow != null)
                            if (modifiedReading != null) modifiedReading.Flow = flow / getDmaDataResults.Count(x => x.Flow != null);
                        if (pressure != null)
                            if (modifiedReading != null)
                                modifiedReading.Pressure = pressure / getDmaDataResults.Count(x => x.Pressure != null);
                        if (volume != null)
                            if (modifiedReading != null) modifiedReading.Volume = volume;

                        if (modifiedReading != null)
                        {
                            modifiedReading.Time = thisBlock.From;

                            subResults.Add(modifiedReading);
                        }
                    }
                }

                allResults = subResults;
            }

            var resultTxt = new StringBuilder();
            foreach (var thisReading in allResults)
            {
                var flowValue = thisReading.Flow != null ?
                    "<variable name=\"flow\">" + $"<value>{thisReading.Flow}</value>" + "</variable>"
                    : null;

                var pressureValue = thisReading.Pressure != null ?
                    "<variable name=\"pressure\">" + $"<value>{thisReading.Pressure}</value>" + "</variable>"
                    : null;

                var volumeValue = thisReading.Volume != null ?
                    "<variable name=\"volume\">" + $"<value>{thisReading.Volume}</value>" + "</variable>"
                    : null;

                resultTxt.Append($"<record><time>{thisReading.Time.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</time>{flowValue}{pressureValue}{volumeValue}</record>{Environment.NewLine}");
            }

            var dmaName = string.Empty;
            if (resultTxt.Length > 0)
            {
                var firstOrDefault = _currentContext.DMAs.FirstOrDefault(x => x.Identifier == elementId);
                if (firstOrDefault != null)
                    dmaName = firstOrDefault.Name;
            }
            return "<response><recordSet>" + $"<elementId>{elementId}</elementId>" +
                              $"<name>{dmaName}</name>{resultTxt}</recordSet>" + "</response>";
        }

        public void Create()
        {
            _currentContext = new SQLAzureDataContext();

            var dma = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;

            foreach (var thisRecord in _currentRequestManager.Records)
            {
                var time = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "time").Value);
                
                float? pressure = null;
                if (thisRecord.Count(kvp => kvp.Key == "pressure_value") != 0)
                {
                    pressure = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "pressure_value").Value);
                }

                float? volume = null;
                if (thisRecord.Count(kvp => kvp.Key == "volume_value") != 0)
                {
                    volume = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "volume_value").Value);
                }

                float? flow = null;
                if (thisRecord.Count(kvp => kvp.Key == "flow_value") != 0)
                {
                    flow = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "flow_value").Value);
                }
                else
                {
                    if (volume != null)
                    {
                        flow = (volume * 1000) / (15 * 60);
                    }
                }

                _currentContext.InsertDMAData(dma, time, flow, volume, pressure);
            }
        }
    }
}
