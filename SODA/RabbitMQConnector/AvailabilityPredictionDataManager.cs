using DataAccess;
using System;
using System.Linq;
using System.Text;
using static System.DateTimeOffset;

namespace RabbitMQConnector
{
    public class AvailabilityPredictionDataManager
    {
        readonly RequestManager _currentRequestManager;
        SQLAzureDataContext _currentContext;

        public AvailabilityPredictionDataManager(RequestManager requestManager)
        {
            _currentRequestManager = requestManager;
        }

        public string Read()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId  = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start      = Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value);
            var end        = Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value);
            var allResults = _currentContext.GetAvailabilityPredictionData(start, end, elementId).ToList();

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

                var contributingVolumeValue = thisReading.Contributing_volume != null ?
                    $"<variable name=\"contributing_volume\">{$"<value>{thisReading.Contributing_volume}</value>"}</variable>"
                    : null;

                resultTxt.Append("<record>" + $"<from>{thisReading.From.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</from>" +
                             $"<to>{thisReading.To.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</to>{contributingVolumeValue}</record>{Environment.NewLine}");
            }

            string reservoirName = string.Empty;
            if (resultTxt.Length > 0)
            {
                reservoirName = _currentContext.Reservoirs.FirstOrDefault(x => x.Identifier == elementId)?.Name;
            }

            return "<response>" + "<recordSet>" + $"<elementId>{elementId}</elementId>" +
                           $"<name>{reservoirName}</name>" +
                           $"<metadata baseTime=\"{baseTime}\" creationTime=\"{creationTime}\" generatedBy=\"{generatedBy}\" comment=\"{comment}\" />{resultTxt}</recordSet>" +
                           "</response>";
        }

        public void Create()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId    = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var baseTime     = Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "baseTime").Value);
            var creationTime = Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "creationTime").Value);
            var generatedBy  = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "generatedBy").Value;
            var comment      = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "comment").Value;
            
            foreach (var thisRecord in _currentRequestManager.Records)
            {
                var from = Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "from").Value);
                var to   = Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "to").Value);
                
                float? contributingVolume = null;
                if (thisRecord.Count(kvp => kvp.Key == "contributing_volume_value") > 0)
                {
                    contributingVolume = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "contributing_volume_value").Value);
                }

                var application = _currentContext.Applications.FirstOrDefault(x => x.Identifier == generatedBy);

                var availabilityPredictionData = new AvailabilityPredictionData
                {
                    Reservoir = _currentContext.Reservoirs.FirstOrDefault(x => x.Identifier == elementId),
                    From = @from,
                    To = to,
                    CreationTime = creationTime,
                    Application = application,
                    Comment = comment,
                    BaseTime = baseTime,
                    Contributing_volume = contributingVolume
                };

                _currentContext.AvailabilityPredictionDatas.InsertOnSubmit(availabilityPredictionData);
            }

            _currentContext.SubmitChanges();
        }
    }
}
