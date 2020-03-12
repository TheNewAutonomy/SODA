using DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static System.Int32;

namespace RabbitMQConnector
{
    public class Regressor
    {
        public string RegressorType { get; set; }
        public double RegressorValue { get; set; }
        public double ScaledRegressor { get; set; }
    }

    public class Alpha
    {
        public string AlphaType { get; set; }
        public double Mu { get; set; }
        public double Ve { get; set; }
    }

    public class DemandPredictionDataManager
    {
        readonly RequestManager _currentRequestManager;
        SQLAzureDataContext _currentContext;

        public DemandPredictionDataManager(RequestManager requestManager)
        {
            _currentRequestManager = requestManager;
        }

        public string GetNearestNeighboursResponse()
        {
            _currentContext = new SQLAzureDataContext();

            var elementIdString = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var elementId       = _currentContext.DMAs.FirstOrDefault(x => x.Identifier == elementIdString).Id;
            var numberOfNearest = Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "numberOfNearest").Value);
            var regressorString = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "regressor").Value;
            var regressorSet    = regressorString.Split(';');

            var alphaStr = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "alpha").Value;
            var alphaSet = alphaStr.Split(';');

            // Extract the regressor values
            var regressors = regressorSet.Select(regressor => regressor.Split(',')).Select(regressorContent => new Regressor
            {
                RegressorType = regressorContent[0], RegressorValue = double.Parse(regressorContent[1])
            }).ToList();

            // Extract the alpha values
            var alphas = alphaSet.Select(alpha => alpha.Split(',')).Select(alphaContent => new Alpha
            {
                AlphaType = alphaContent[0], Mu = double.Parse(alphaContent[1]), Ve = double.Parse(alphaContent[2])
            }).ToList();

            double inputDateTimeDay = DateTimeOffset.Now.Day;
            double inputDateTimeYear = DateTimeOffset.Now.Year;

            foreach (var regressor in regressors)
            {
                switch (regressor.RegressorType)
                {
                    case "tau_s_d":
                        inputDateTimeDay = Math.Asin(regressor.RegressorValue);
                        break;
                    case "tau_c_d":
                        inputDateTimeDay = Math.Acos(regressor.RegressorValue);
                        break;
                    case "tau_s_w":
                        break;
                    case "tau_c_w":
                        break;
                    case "tau_s_y":
                        inputDateTimeYear = Math.Asin(regressor.RegressorValue);
                        break;
                    case "tau_c_y":
                        inputDateTimeYear = Math.Acos(regressor.RegressorValue);
                        break;
                    default:
                        break;
                }
            }

            var inputDateTime = new DateTime(DateTimeOffset.Now.Year, 1, 1);
            inputDateTime = inputDateTime.AddDays((int)inputDateTimeYear);
            inputDateTime = inputDateTime.AddHours((int)inputDateTimeDay);

            var resultTxt = new StringBuilder();

            // Scale the regressors
            foreach (var regressor in regressors)
            {
                var returnRegressor = "<predictor>";

                var matchedAlpha = alphas.FirstOrDefault(x => string.CompareOrdinal(x.AlphaType, regressor.RegressorType) == 0);
                regressor.ScaledRegressor = regressor.RegressorValue * matchedAlpha.Mu + matchedAlpha.Ve;

                if (regressor.RegressorType.StartsWith("x1"))
                {
                    var results = _currentContext.FindNearestDemandPrediction(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x1)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x2"))
                {
                    var results = _currentContext.FindNearestWeatherPrecipitation(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x2)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x3"))
                {
                    var results = _currentContext.FindNearestWeatherTemperature(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x3)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x4"))
                {
                    var results = _currentContext.FindNearestWeatherHumidity(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x4)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x5"))
                {
                    var results = _currentContext.FindNearestWeatherPressure(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x5)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x6"))
                {
                    var results = _currentContext.FindNearestWeatherWindVelocity(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x6)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x7"))
                {
                    var results = _currentContext.FindNearestWeatherWindDirection(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x7)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x8"))
                {
                    var results = _currentContext.FindNearestWeatherSolarRadiation(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x8)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x9"))
                {
                    var results = _currentContext.FindNearestWeatherForecastPrecipitation(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x9)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x10"))
                {
                    var results = _currentContext.FindNearestWeatherForecastTemperature(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x10)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x11"))
                {
                    var results = _currentContext.FindNearestWeatherForecastHumidity(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x11)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x12"))
                {
                    var results = _currentContext.FindNearestWeatherForecastPressure(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x12)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x13"))
                {
                    var results = _currentContext.FindNearestWeatherForecastWindVelocity(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x13)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x14"))
                {
                    var results = _currentContext.FindNearestWeatherForecastWindDirection(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x14)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x15"))
                {
                    var results = _currentContext.FindNearestWeatherForecastSolarRadiation(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x15)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x16"))
                {
                    var results = _currentContext.FindNearestDMAflow(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x16)" + newregressor.Scaled_Regressor + ","));
                }
                else if (regressor.RegressorType.StartsWith("x17"))
                {
                    var results = _currentContext.FindNearestDMApressure(elementId, numberOfNearest, matchedAlpha.Mu, matchedAlpha.Ve, regressor.RegressorValue, inputDateTime);

                    returnRegressor = results.Aggregate(returnRegressor, (current, newregressor) => current + ("(x17)" + newregressor.Scaled_Regressor + ","));
                }

                if (returnRegressor.EndsWith(","))
                {
                    returnRegressor = returnRegressor.Remove(returnRegressor.Length - 1, 1);
                }
                returnRegressor += "</predictor>";

             
                resultTxt.Append("<record>" +
                             $"<predictorTime>{inputDateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</predictorTime>{returnRegressor}/<record>{Environment.NewLine}");
            }

            return "<response><recordSet>" +
                              $"<elementId>{elementIdString}</elementId>{resultTxt}</recordSet>" + "</response>";
        }

        public string ReadSpecificProperty(List<int> indices)
        {
            _currentContext = new SQLAzureDataContext();
            var elementId   = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start       = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "start") ?
                              DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value) : DateTimeOffset.MinValue;
            var end         = _currentRequestManager.RootElements.Any(kvp => kvp.Key == "end") ?
                              DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value) : DateTimeOffset.MaxValue;
            long timestep   = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "timeStep") > 0 ?
                              Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;
            
            var allResults = _currentContext.GetDemandPredictionData(start, end, elementId).ToList();

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

                var subResults = new List<GetDemandPredictionDataResult>();

                // loop through each block and and find any records that span that block, trimming those on the edges
                foreach (var thisBlock in dateTimeBlockSet)
                {
                    var firstRecordkInSet = allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From).OrderBy(x => x.From).FirstOrDefault();
                    var lastRecordInSet = allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From).OrderBy(x => x.From).Reverse().LastOrDefault();

                    if (firstRecordkInSet != null && lastRecordInSet != null)
                    {
                        var recordsSpanningBlock = allResults.Where(x => x.From >= firstRecordkInSet.From && x.To <= lastRecordInSet.To);

                        var getDemandPredictionDataResults = recordsSpanningBlock as GetDemandPredictionDataResult[] ?? recordsSpanningBlock.ToArray();
                        if (getDemandPredictionDataResults.Any())
                        {
                            var trimmedrecordsSpanningBlock = new List<GetDemandPredictionDataResult>();
                            foreach (var record in getDemandPredictionDataResults)
                            {
                                double percentageToTrim = 0;

                                if (record.From < thisBlock.From)
                                {
                                    var currentBlockTicks = record.To.Ticks - record.From.Ticks;
                                    var ticksToTrim = thisBlock.From.Ticks - record.From.Ticks;

                                    percentageToTrim += (double)(((double)100.0 / (double)currentBlockTicks) * (double)ticksToTrim);
                                }
                                if (record.To > thisBlock.To)
                                {
                                    var currentBlockTicks = record.To.Ticks - record.From.Ticks;
                                    var ticksToTrim = record.To.Ticks - thisBlock.To.Ticks;

                                    percentageToTrim = (double)(((double)100.0 / (double)currentBlockTicks) * (double)ticksToTrim);
                                }

                                record.Demand = record.Demand - ((record.Demand / 100) * percentageToTrim);

                                if (record.Uncertainty != null)
                                {
                                    record.Uncertainty = record.Uncertainty - ((record.Uncertainty / 100) * percentageToTrim);
                                }

                                trimmedrecordsSpanningBlock.Add(record);
                            }

                            var demand = 0.0;
                            double? uncertainty = null;

                            foreach (var reading in trimmedrecordsSpanningBlock)
                            {
                                demand += reading.Demand;

                                if (reading.Uncertainty != null)
                                {
                                    if (uncertainty == null)
                                    {
                                        uncertainty = reading.Uncertainty;
                                    }
                                    else
                                    {
                                        uncertainty += reading.Uncertainty;
                                    }
                                }
                            }

                            var modifiedReading = trimmedrecordsSpanningBlock.FirstOrDefault();

                            if (modifiedReading != null)
                            {
                                modifiedReading.Demand = demand / trimmedrecordsSpanningBlock.Count();
                                if (uncertainty != null)
                                    modifiedReading.Uncertainty = uncertainty;

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
                var demandValue = string.Empty;

                if (indices.Contains(19))
                {
                    demandValue = "<variable name=\"demand\">" + $"<value>{thisReading.Demand}</value>" + "</variable>";
                }

                if (!string.IsNullOrEmpty(demandValue))
                {
                    var metadata = "<metadata baseTime=\"" +
                                   thisReading.BaseTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK") +
                                   "\" creationTime=\"" +
                                   thisReading.CreationTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK") +
                                   "\" generatedBy=\"" + thisReading.Application + "\" comment=\"" +
                                   thisReading.Comment + "\" />";

                    // 2015-02-01T00:15:00+01:00
                    resultTxt.Append($"<record>{metadata}<from>{thisReading.From.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</from>" +
                        $"<to>{thisReading.To.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</to>{demandValue}</record>{Environment.NewLine}");
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
                            Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;
            
            var allResults = _currentContext.GetDemandPredictionData(start, end, elementId).ToList();

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

                var subResults = new List<GetDemandPredictionDataResult>();

                // loop through each block and and find any records that span that block, trimming those on the edges
                foreach (var thisBlock in dateTimeBlockSet)
                {
                    var firstRecordkInSet = allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From).OrderBy(x => x.From).FirstOrDefault();
                    var lastRecordInSet = allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From).OrderBy(x => x.From).LastOrDefault();

                    if (firstRecordkInSet != null && lastRecordInSet != null)
                    {
                        var recordsSpanningBlock = allResults.Where(x => x.From >= firstRecordkInSet.From && x.To <= lastRecordInSet.To);

                        var getDemandPredictionDataResults = recordsSpanningBlock as GetDemandPredictionDataResult[] ?? recordsSpanningBlock.ToArray();
                        if (getDemandPredictionDataResults.Any())
                        {
                            var trimmedrecordsSpanningBlock = new List<GetDemandPredictionDataResult>();
                            foreach (var record in getDemandPredictionDataResults)
                            {
                                double percentageToTrim = 0;

                                if (record.From < thisBlock.From)
                                {
                                    var currentBlockTicks = record.To.Ticks - record.From.Ticks;
                                    var ticksToTrim = thisBlock.From.Ticks - record.From.Ticks;
                                    
                                    percentageToTrim += (double)(((double)100.0 / (double)currentBlockTicks) * (double)ticksToTrim);
                                }
                                if (record.To > thisBlock.To)
                                {
                                    var currentBlockTicks = record.To.Ticks - record.From.Ticks;
                                    var ticksToTrim = record.To.Ticks - thisBlock.To.Ticks;
                                    
                                    percentageToTrim = (double)(((double)100.0 / (double)currentBlockTicks) * (double)ticksToTrim);
                                }
                                
                                record.Demand = record.Demand - ((record.Demand / 100) * percentageToTrim);

                                if (record.Uncertainty != null)
                                {
                                    record.Uncertainty = record.Uncertainty - ((record.Uncertainty / 100) * percentageToTrim);
                                }

                                trimmedrecordsSpanningBlock.Add(record);
                            }

                            var demand = 0.0;
                            double? uncertainty = null;

                            foreach (var reading in trimmedrecordsSpanningBlock)
                            {
                                demand += reading.Demand;
                                
                                if (reading.Uncertainty != null)
                                {
                                    if (uncertainty == null)
                                    {
                                        uncertainty = reading.Uncertainty;
                                    }
                                    else if (reading.Uncertainty != -1)
                                    {
                                        uncertainty += reading.Uncertainty;
                                    }
                                }
                            }

                            var modifiedReading = trimmedrecordsSpanningBlock.FirstOrDefault();

                            if (modifiedReading != null)
                            {
                                modifiedReading.Demand = demand;
                                if (uncertainty != null)
                                    modifiedReading.Uncertainty = uncertainty;
                           
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
            var baseTime     = string.Empty;
            var creationTime = string.Empty;
            var generatedBy  = string.Empty;
            var comment      = string.Empty;
            
            foreach (var thisReading in allResults)
            {
                if (baseTime == string.Empty)
                {
                    baseTime = thisReading.BaseTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
                    creationTime = thisReading.CreationTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK");
                }

                generatedBy = thisReading.Identifier;

                var demandValue = "<variable name=\"demand\">" + $"<value>{thisReading.Demand}</value>" + "</variable>";

                var uncertaintyValue = thisReading.Uncertainty != null ?
                    "<variable name=\"uncertainty\">" + $"<value>{thisReading.Uncertainty}</value>" + "</variable>"
                    : null;

                resultTxt.Append("<record>" + $"<from>{thisReading?.From.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</from>" +
                             $"<to>{thisReading?.To.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</to>{demandValue}{uncertaintyValue}</record>{Environment.NewLine}");
            }

            var dmaName = string.Empty;
            if (resultTxt.Length > 0)
            {
                var firstOrDefault = _currentContext.DMAs.FirstOrDefault(x => x.Identifier == elementId);
                if (firstOrDefault != null)
                    dmaName = firstOrDefault.Name;
            }
            return "<response>" + "<recordSet>" + $"<elementId>{elementId}</elementId>" +
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
                var from     = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "start").Value);
                var to       = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "end").Value);
                var demand     = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "demand_value").Value);
                float? uncertainty = null;
                if (thisRecord != null && thisRecord.Any(kvp => kvp.Key == "uncertainty_value"))
                {
                    uncertainty = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "uncertainty_value").Value);
                }

                var application = _currentContext.Applications.FirstOrDefault(x => x.Identifier == generatedBy);

                DemandPredictionData demandPredictionData = new DemandPredictionData
                {
                    DMA = _currentContext.DMAs.FirstOrDefault(x => x.Identifier == elementId),
                    From = from,
                    To = to,
                    Demand = demand,
                    Uncertainty = uncertainty,
                    BaseTime = baseTime,
                    CreationTime = creationTime,
                    Application = application,
                    Comment = comment
                };

                _currentContext.DemandPredictionDatas.InsertOnSubmit(demandPredictionData);
            }

            _currentContext.SubmitChanges();
        }
    }
}
