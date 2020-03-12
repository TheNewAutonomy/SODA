using DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RabbitMQConnector
{
    public class PricingDataManager
    {
        readonly RequestManager _currentRequestManager;
        SQLAzureDataContext _currentContext;

        public PricingDataManager(RequestManager requestManager)
        {
            _currentRequestManager = requestManager;
        }

        public string Read()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value);
            var end = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value);
            var simulation = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "simulation") ?
                             DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "simulation").Value) : DateTimeOffset.Now;
            long timestep = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "timeStep") > 0 ?
                             int.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;

            var allResults = _currentContext.GetPricingData(start, end, elementId, simulation).ToList();

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
                        From = start.AddTicks(blockIndex * timestepTicks),
                        To = start.AddTicks((blockIndex + 1) * timestepTicks)
                    };

                    dateTimeBlockSet.Add(newDateTimeBlock);
                }

                var subResults = new List<GetPricingDataResult>();

                // loop through each block and and find any records that span that block, trimming those on the edges
                foreach (DateTimeBlock thisBlock in dateTimeBlockSet)
                {
                    var firstRecordkInSet = allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From).OrderBy(x => x.From).FirstOrDefault();
                    var lastRecordInSet = allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From).OrderBy(x => x.From).LastOrDefault();

                    if (firstRecordkInSet != null && lastRecordInSet != null)
                    {
                        var recordsSpanningBlock = allResults.Where(x => x.From >= firstRecordkInSet.From && x.To <= lastRecordInSet.To);

                        var getPricingDataResults = recordsSpanningBlock as GetPricingDataResult[] ?? recordsSpanningBlock.ToArray();
                        if (getPricingDataResults.Any())
                        {
                            decimal Price = getPricingDataResults.Sum(reading => reading.Price);

                            var modifiedReading = getPricingDataResults.FirstOrDefault();

                            if (modifiedReading != null)
                            {
                                modifiedReading.Price = Price / getPricingDataResults.Count();

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
            var baseTime = string.Empty;
            var creationTime = string.Empty;
            var generatedBy = string.Empty;

            foreach (var thisReading in allResults)
            {
                if (baseTime == string.Empty)
                {
                    baseTime = thisReading.BaseTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
                    creationTime = thisReading.CreationTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
                }

                generatedBy = thisReading.Identifier;

                resultTxt.Append("<record>" + $"<from>{thisReading.From.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</from>" +
                             $"<to>{thisReading.To.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</to>" +
                             "<variable name=\"price\">" + $"<value>{thisReading.Price}</value>" + "</variable>" +
                             $"</record>{Environment.NewLine}");
            }

            var dmaName = string.Empty;
            if (resultTxt.Length > 0)
            {
                resultTxt.Insert(0, $"<elementId>{elementId}</elementId>{Environment.NewLine}<name>{dmaName}</name>{Environment.NewLine}" +
                            $"<metadata baseTime=\"{baseTime}\" creationTime=\"{creationTime}\" generatedBy=\"{generatedBy}\" />{Environment.NewLine}");
            }

            return "<response>" + $"<recordSet>{resultTxt}</recordSet>" + "</response>";
        }

        public string Create()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var baseTime = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "baseTime").Value);
            var creationTime = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "creationTime").Value);
            var generatedBy = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "generatedBy").Value;
            var comment = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "comment").Value;
            
            foreach (var thisRecord in _currentRequestManager.Records)
            {
                var fromDateTime = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "start").Value);
                var toDateTime = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "end").Value);
                var price = decimal.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "price_value").Value);

                var application = _currentContext.Applications.FirstOrDefault(x => x.Identifier == generatedBy);

                var pricingData = new PricingData
                {
                    DMA = _currentContext.DMAs.FirstOrDefault(x => x.Identifier == elementId),
                    From = fromDateTime,
                    To = toDateTime,
                    CreationTime = creationTime,
                    Application = application,
                    Comment = comment,
                    BaseTime = baseTime,
                    Price = price
                };

                _currentContext.PricingDatas.InsertOnSubmit(pricingData);
            }

            _currentContext.SubmitChanges();

            return elementId;
        }
    }
}
