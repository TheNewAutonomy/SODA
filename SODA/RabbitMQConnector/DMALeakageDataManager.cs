using DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static System.String;

namespace RabbitMQConnector
{
    public class DMALeakageDataManager
    {
        readonly RequestManager _currentRequestManager;
        SQLAzureDataContext _currentContext;

        public DMALeakageDataManager(RequestManager requestManager)
        {
            _currentRequestManager = requestManager;
        }

        public string ReadSpecificProperty(List<int> indices)
        {
            _currentContext = new SQLAzureDataContext();
            var elementId   = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start       = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "start") ?
                              DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value) : DateTimeOffset.MinValue;
            var end         = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "end") ?
                              DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value) : DateTimeOffset.MaxValue;
            var simulation  = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "simulation") ?
                              DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "simulation").Value) : DateTimeOffset.MinValue;
            long timestep   = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "timeStep") > 0 ?
                              int.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;

            var allResults = _currentContext.DMALeakageDatas.Where(x => x.DMA.Identifier == elementId && x.From >= start && x.To <= end).OrderBy(x => x.CreationTime).ToList();

            var nearestForeCastDateTime = DateTimeOffset.Now;
            if (allResults.Any() &&
                DateTimeOffset.Compare(simulation, DateTimeOffset.MinValue) == 0)
            {
                nearestForeCastDateTime = allResults.Aggregate((x, y) => Math.Abs(x.BaseTime.Ticks - DateTimeOffset.Now.Ticks) < Math.Abs(y.BaseTime.Ticks - DateTimeOffset.Now.Ticks) ? x : y).BaseTime;
            }
            else if (allResults.Any())
            {
                nearestForeCastDateTime = allResults.Aggregate((x, y) => Math.Abs(x.CreationTime.Ticks - simulation.Ticks) < Math.Abs(y.CreationTime.Ticks - simulation.Ticks) ? x : y).CreationTime;
            }

            if (timestep != -1 && allResults.Any())
            {
                var subResults = new List<DMALeakageData>();

                const long multiplier = 10000000;
                long timestepTicks = timestep * multiplier;

                long startTick = allResults.FirstOrDefault().CreationTime.Ticks;
                long endTick = allResults.LastOrDefault().CreationTime.Ticks;
                long counter = startTick;

                while (counter <= endTick)
                {
                    var timeStepResults = allResults.Aggregate((x, y) => Math.Abs(x.CreationTime.Ticks - counter) < Math.Abs(y.CreationTime.Ticks - counter) ? x : y);

                    if (timeStepResults != null)
                    {
                        counter += timestepTicks;
                        subResults.Add(timeStepResults);
                    }
                }

                allResults = subResults.GroupBy(x => x.DMAId).Select(y => y.First()).Where(y => y.BaseTime == nearestForeCastDateTime).ToList();
            }
            else if (allResults.Any() && allResults.Select(x => x.CreationTime).Distinct().Count() > 1)
            {
                var subResults = allResults.Where(y => y.BaseTime == nearestForeCastDateTime).ToList();
                allResults = subResults;
            }

            var resultTxt = new StringBuilder();
            foreach (var thisReading in allResults)
            {
                var lossValue = Empty;

                if (indices.Contains(2))
                {
                    lossValue = thisReading.Real_losses_value != null ?
                                        Format("<variable name=\"value\">" + "<value>{0}</value>" + "<type>{0}</type>" + "</variable>", thisReading.Real_losses_value) : null;
                }

                if (!IsNullOrEmpty(lossValue))
                {
                    var metadata =
                        $"<metadata baseTime=\"{thisReading.BaseTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}\" creationTime=\"{thisReading.CreationTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}\" generatedBy=\"{thisReading.Application.Identifier}\" comment=\"{thisReading.Comment}\" />";
                    
                    resultTxt.Append($"<record>{metadata}<from>{thisReading?.From.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</from>" +
                        $"<to>{thisReading?.To.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</to>{lossValue}</record>{Environment.NewLine}");
                }
            }

            return resultTxt.ToString();
        }

        public string Read()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId  = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start      = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value);
            var end        = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value);
            var simulation = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "simulation") ?
                             DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "simulation").Value) : DateTimeOffset.MinValue;
            long timestep  = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "timeStep") > 0 ?
                             int.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;

            var allResults = _currentContext.DMALeakageDatas.Where(x => x.DMA.Identifier == elementId && x.From >= start && x.To <= end).OrderBy(x => x.CreationTime).ToList();

            var nearestForeCastDateTime = DateTimeOffset.Now;
            if (allResults.Any() &&
                DateTimeOffset.Compare(simulation, DateTimeOffset.MinValue) == 0)
            {
                nearestForeCastDateTime = allResults.Aggregate((x, y) => Math.Abs(x.BaseTime.Ticks - DateTimeOffset.Now.Ticks) < Math.Abs(y.BaseTime.Ticks - DateTimeOffset.Now.Ticks) ? x : y).BaseTime;
            }
            else if (allResults.Any())
            {
                nearestForeCastDateTime = allResults.Aggregate((x, y) => Math.Abs(x.CreationTime.Ticks - simulation.Ticks) < Math.Abs(y.CreationTime.Ticks - simulation.Ticks) ? x : y).CreationTime;
            }

            if (timestep != -1 && allResults.Any())
            {
                var subResults = new List<DMALeakageData>();

                const long multiplier = 10000000;
                long timestepTicks = timestep * multiplier;

                long startTick = allResults.FirstOrDefault().CreationTime.Ticks;
                long endTick = allResults.LastOrDefault().CreationTime.Ticks;
                long counter = startTick;

                while (counter <= endTick)
                {
                    // var TimeStepResults = allResults.OrderBy(item => Math.Abs(counter - item.CreationTime.Ticks)).First();
                    var timeStepResults = allResults.Aggregate((x, y) => Math.Abs(x.CreationTime.Ticks - counter) < Math.Abs(y.CreationTime.Ticks - counter) ? x : y);

                    if (timeStepResults != null)
                    {
                        counter += timestepTicks;
                        subResults.Add(timeStepResults);
                    }
                }

                allResults = subResults.GroupBy(x => x.DMAId).Select(y => y.First()).Where(y => y.BaseTime == nearestForeCastDateTime).ToList();
            }
            else if (allResults.Any() && allResults.Select(x => x.CreationTime).Distinct().Count() > 1)
            {
                var subResults = allResults.Where(y => y.BaseTime == nearestForeCastDateTime).ToList();
                allResults = subResults;
            }

            var resultTxt = new StringBuilder();
            var baseTime     = Empty;
            var creationTime = Empty;
            var generatedBy  = Empty;
            var comment      = Empty;

            foreach (var thisReading in allResults)
            {
                if (baseTime == Empty)
                {
                    baseTime = thisReading.BaseTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
                    creationTime = thisReading.CreationTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
                }

                generatedBy = thisReading.Application.Identifier;
                comment = thisReading.Comment;

                var realLossesValue = thisReading.Real_losses_value != null ?
                    "<variable name=\"real_losses\">" + $"<value>{thisReading.Real_losses_value}</value>" +
                    $"<type>{thisReading.Real_losses_Type}</type>" + "</variable>"
                    : null;
                
                resultTxt.Append("<record>" + $"<from>{thisReading?.From.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</from>" +
                             $"<to>{thisReading?.To.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</to>{realLossesValue}</record>{Environment.NewLine}");
            }

            var dmaName = Empty;
            if (resultTxt.Length > 0)
            {
                var firstOrDefault = _currentContext.DMAs.FirstOrDefault(x => x.Identifier == elementId);
                if (firstOrDefault != null)
                    dmaName = firstOrDefault.Name;
            }

            return "<response><recordSet>" + $"<elementId>{elementId}</elementId>" +
                           $"<name>{dmaName}</name>" +
                           $"<metadata baseTime=\"{baseTime}\" creationTime=\"{creationTime}\" generatedBy=\"{generatedBy}\" comment=\"{comment}\" />{resultTxt}</recordSet>" +
                           "</response>";
        }

        public void Create()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId    = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var baseTime     = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "baseTime").Value);
            var creationTime = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "creationTime").Value);
            var generatedBy  = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "generatedBy").Value;
            var comment      = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "comment").Value;

            foreach (var thisRecord in _currentRequestManager.Records)
            {
                var from           = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "from").Value);
                var to             = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "to").Value);

                float? realLossesValue = null;
                if (thisRecord.Count(kvp => kvp.Key == "real_losses_value") != 0)
                {
                    realLossesValue = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "real_losses_value").Value);
                }

                var realLossesType = thisRecord.Count(kvp => kvp.Key == "real_losses_type") != 0 ?
                                     thisRecord.FirstOrDefault(kvp => kvp.Key == "real_losses_type").Value : Empty;

                var application = _currentContext.Applications.FirstOrDefault(x => x.Identifier == generatedBy);

                var dmaLeakageData = new DMALeakageData
                {
                    DMA = _currentContext.DMAs.FirstOrDefault(x => x.Identifier == elementId),
                    From = @from,
                    To = to,
                    CreationTime = creationTime,
                    Application = application,
                    Comment = comment,
                    BaseTime = baseTime,
                    Real_losses_value = realLossesValue,
                    Real_losses_Type = realLossesType
                };

                _currentContext.DMALeakageDatas.InsertOnSubmit(dmaLeakageData);
            }

            _currentContext.SubmitChanges();
        }
    }
}
