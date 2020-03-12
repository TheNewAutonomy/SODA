using DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RabbitMQConnector
{
    public class TankDataManager
    {
        readonly RequestManager _currentRequestManager;
        SQLAzureDataContext _currentContext;

        public TankDataManager(RequestManager requestManager)
        {
            _currentRequestManager = requestManager;
        }

        public string Read()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start     = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value);
            var end       = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value);
            long timestep = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "timeStep") > 0 ?
                            int.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;

            var allResults = _currentContext.GetTankData(start, end, elementId).ToList();

            if (timestep != -1 && allResults.Any())
            {
                const long multiplier = 10000000;
                long timestepTicks = timestep * multiplier;
                long numberOfTicksBetweenStartAndEnd = (long)(end.Ticks - start.Ticks);
                int numberOfBlock = (int)(numberOfTicksBetweenStartAndEnd / timestepTicks);

                // Create a collection of fixed blocks in time from supplied Start to End
                List<DateTimeBlock> dateTimeBlockSet = new List<DateTimeBlock>();
                for (var BlockIndex = 0; BlockIndex < numberOfBlock; BlockIndex++)
                {
                    var newDateTimeBlock = new DateTimeBlock
                    {
                        From = start.AddTicks(BlockIndex*timestepTicks),
                        To = start.AddTicks((BlockIndex + 1)*timestepTicks)
                    };

                    dateTimeBlockSet.Add(newDateTimeBlock);
                }

                var subResults = new List<GetTankDataResult>();

                // loop through each block and and find any records that span that block, trimming those on the edges
                foreach (var thisBlock in dateTimeBlockSet)
                {
                    var firstRecordkInSet = allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From).OrderBy(x => x.From).FirstOrDefault();
                    var lastRecordInSet = allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From).OrderBy(x => x.From).Reverse().LastOrDefault();

                    if (firstRecordkInSet != null && lastRecordInSet != null)
                    {
                        var recordsSpanningBlock = allResults.Where(x => x.From >= firstRecordkInSet.From && x.To <= lastRecordInSet.To);

                        var getTankDataResults = recordsSpanningBlock as GetTankDataResult[] ?? recordsSpanningBlock.ToArray();
                        if (getTankDataResults.Any())
                        {
                            var trimmedrecordsSpanningBlock = new List<GetTankDataResult>();
                            foreach (var record in getTankDataResults)
                            {
                                double percentageToTrim = 0;

                                if (record.From < thisBlock.From)
                                {
                                    long currentBlockTicks = record.To.Ticks - record.From.Ticks;
                                    long ticksToTrim = thisBlock.From.Ticks - record.From.Ticks;
                                    
                                    percentageToTrim += (double)(((double)100.0 / (double)currentBlockTicks) * (double)ticksToTrim);
                                }
                                if (record.To > thisBlock.To)
                                {
                                    long currentBlockTicks = record.To.Ticks - record.From.Ticks;
                                    long ticksToTrim = record.To.Ticks - thisBlock.To.Ticks;
                                    
                                    percentageToTrim = (double)(((double)100.0 / (double)currentBlockTicks) * (double)ticksToTrim);
                                }

                                if (record.Inflow != null)
                                {
                                    record.Inflow = record.Inflow - ((record.Inflow / 100) * percentageToTrim);
                                }
                                trimmedrecordsSpanningBlock.Add(record);
                            }

                            double? inflow = null;

                            foreach (var reading in trimmedrecordsSpanningBlock)
                            {
                                if (reading.Inflow != null)
                                {
                                    if (inflow == null)
                                    {
                                        inflow = reading.Inflow;
                                    }
                                    else
                                    {
                                        inflow += reading.Inflow;
                                    }
                                }
                            }

                            var modifiedReading = trimmedrecordsSpanningBlock.FirstOrDefault();

                            if (inflow != null)
                                if (modifiedReading != null)
                                    modifiedReading.Inflow = inflow / trimmedrecordsSpanningBlock.Count();

                            if (modifiedReading != null)
                            {
                                modifiedReading.From = thisBlock.From;
                                modifiedReading.To = thisBlock.To;

                                subResults.Add(modifiedReading);
                            }
                        }
                    }
                }

                allResults = subResults;
            }

            var resultTxt = new StringBuilder();

            foreach (var thisReading in allResults)
            {
                var volumeValue = thisReading.Volume != null ?
                    "<variable name=\"volume\">" + $"<value>{thisReading.Volume}</value>" + "</variable>"
                    : null;

                var levelValue = thisReading.Outflow != null ?
                    "<variable name=\"outflow\">" + $"<value>{thisReading.Outflow}</value>" + "</variable>"
                    : null;

                var inflowValue = thisReading.Inflow != null ?
                    "<variable name=\"inflow\">" + $"<value>{thisReading.Inflow}</value>" + "</variable>"
                    : null;

                resultTxt.Append("<record>" + $"<from>{thisReading?.From.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</from>" +
                             $"<to>{thisReading?.To.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</to>{volumeValue}{levelValue}{inflowValue}</record>{Environment.NewLine}");
            }

            var tankName = string.Empty;
            if (resultTxt.Length > 0)
            {
                tankName = _currentContext.Tanks.FirstOrDefault(x => x.Identifier == elementId)?.Name;
            }

            return "<response>" + "<recordSet>" + $"<elementId>{elementId}</elementId>" +
                              $"<name>{tankName}</name>{resultTxt}</recordSet>" + "</response>";
        }

        public void Create()
        {
            _currentContext = new SQLAzureDataContext();

            foreach (var thisRecord in _currentRequestManager.Records)
            {
                var elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
                var from      = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "from").Value);
                var to        = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "to").Value);
                float? volume = null;
                if (thisRecord.Count(kvp => kvp.Key == "volume_value") != 0)
                {
                    volume = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "volume_value").Value);
                }

                float? level = null;
                if (thisRecord.Count(kvp => kvp.Key == "outflow_value") != 0)
                {
                    level = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "outflow_value").Value);
                }

                float? inflow = null;
                if (thisRecord.Count(kvp => kvp.Key == "inflow_value") != 0)
                {
                    inflow = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "inflow_value").Value);
                }

                _currentContext.InsertTankData(elementId, from, to, volume, level, inflow);
            }

            _currentContext.SubmitChanges();
        }
    }
}
