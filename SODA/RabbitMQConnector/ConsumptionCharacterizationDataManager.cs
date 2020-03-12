using DataAccess;
using System;
using System.Linq;
using System.Text;
using static System.DateTimeOffset;
using static System.String;

namespace RabbitMQConnector
{
    public class ConsumptionCharacterizationDataManager
    {
        readonly RequestManager _currentRequestManager;
        private SQLAzureDataContext _currentContext;

        public ConsumptionCharacterizationDataManager(RequestManager requestManager)
        {
            _currentRequestManager = requestManager;
        }

        public string Readdicmsite()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start     = Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value);
            var end       = Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value);

            var allResults = _currentContext.GetDICMSite(start, end, elementId).ToList();

            var resultTxt = Empty;
            foreach (var thisReading in allResults)
            {
                var volumeValue = thisReading.Volume != null ?
                    "<variable name=\"volume\">" + $"<value>{thisReading.Volume}</value>" + "</variable>"
                    : null;

                var durationValue = thisReading.Duration != null ?
                    "<variable name=\"duration\">" + $"<value>{thisReading.Duration}</value>" + "</variable>"
                    : null;

                if (!(IsNullOrEmpty(volumeValue) &&
                    IsNullOrEmpty(durationValue)))
                {
                    resultTxt += $"<record>{volumeValue}{durationValue}</record>{Environment.NewLine}";
                }
            }

            var response = "<response><recordSet>" + $"<elementId>{elementId}</elementId>" +
                           $"<from>{start}</from>" + $"<to>{end}</to>{resultTxt}</recordSet>" + "</response>";

            return response;
        }

        public string Read()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start     = Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value);
            var end       = Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value);

            var allResults = _currentContext.ConsumptionCharacterizationDatas.Where(x => x.MeterId == elementId && x.From >= start && x.To <= end);

            var resultTxt = new StringBuilder();
            foreach (var thisReading in allResults)
            {
                var volumeValue = thisReading.Volume != null ?
                    "<variable name=\"volume\">" + $"<value>{thisReading.Volume}</value>" + "</variable>"
                    : null;

                var durationValue = thisReading.Duration != null ?
                    "<variable name=\"duration\">" + $"<value>{thisReading.Duration}</value>" + "</variable>"
                    : null;

                var maxFlowValue = thisReading.Max_flow != null ?
                    "<variable name=\"Max_flow\">" + $"<value>{thisReading.Max_flow}</value>" + "</variable>"
                    : null;

                var mostFreqFlowValue = thisReading.Most_freq_flow_value != null && thisReading.Most_freq_flow_Number != null ?
                    "<variable name=\"most_freq_flow\">" + $"<value>{thisReading.Most_freq_flow_value}</value>" +
                    $"<number>{thisReading.Most_freq_flow_Number}</number>" + "</variable>"
                    : null;

                var characterizedDeviceValue = !IsNullOrEmpty(thisReading.characterized_device_value) && thisReading.characterized_device_id != null ?
                    "<variable name=\"characterized_device\">" +
                    $"<value>{thisReading.characterized_device_value}</value>" +
                    $"<id>{thisReading.characterized_device_id}</id>" + "</variable>"
                    : null;

                var timeSinValue = thisReading.time_sin != null ?
                    "<variable name=\"time_sin\">" + $"<value>{thisReading.time_sin}</value>" + "</variable>"
                    : null;

                var timeCosValue = thisReading.time_cos != null ?
                    "<variable name=\"time_cos\">" + $"<value>{thisReading.time_cos}</value>" + "</variable>"
                    : null;

                string confirmedDevice = null;

                if (CompareOrdinal(thisReading.confirmation.ToLower(), "yes") == 0)
                {
                    confirmedDevice = !IsNullOrEmpty(thisReading.confirmed_device_value) && thisReading.confirmed_device_value != null ?
                        "<variable name=\"confirmed_device\">" +
                        $"<value>{thisReading.confirmed_device_value}</value>" +
                        $"<id>{thisReading.confirmed_device_id}</id></variable>"
                        : null;
                }
                else if (CompareOrdinal(thisReading.confirmation.ToLower(), "no") == 0)
                {
                    confirmedDevice = !IsNullOrEmpty(thisReading.characterized_device_value) && thisReading.characterized_device_id != null ?
                        "<variable name=\"confirmed_device\">" + $"<value>{thisReading.characterized_device_value}</value>" +
                        $"<id>{thisReading.characterized_device_id}</id></variable>"
                        : null;
                }
                else if (CompareOrdinal(thisReading.confirmation.ToLower(), "?") == 0)
                {
                    confirmedDevice ="<variable name=\"confirmed_device\"><value>unknown</value><id>unknown</id></variable>";
                }

                var confirmation = !IsNullOrEmpty(thisReading.confirmation) ?
                    "<variable name=\"confirmation\">" + $"<value>{thisReading.confirmation}</value>" + "</variable>"
                    : null;

                resultTxt.Append("<record>" + $"<from>{thisReading.From.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</from>" +
                             $"<to>{thisReading.To.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</to>{volumeValue}{durationValue}{maxFlowValue}{mostFreqFlowValue}{timeSinValue}{timeCosValue}{characterizedDeviceValue}{confirmedDevice}{confirmation}</record>{Environment.NewLine}");
            }

            return "<response>" +
                           $"<recordSet>{$"<elementId>{elementId}</elementId>{resultTxt}</recordSet>"}</response>";
        }

        public void Create()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;

            foreach (var thisRecord in _currentRequestManager.Records)
            {
                var from = Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "start").Value);
                var to = Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "end").Value);

                float? Volume = null;
                if (thisRecord.Count(kvp => kvp.Key == "volume_value") != 0)
                {
                    Volume = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "volume_value").Value);
                }

                float? duration = null;
                if (thisRecord.Count(kvp => kvp.Key == "duration_value") != 0)
                {
                    duration = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "duration_value").Value);
                }

                float? maxFlow = null;
                if (thisRecord.Count(kvp => kvp.Key == "max_flow_value") != 0)
                {
                    maxFlow = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "max_flow_value").Value);
                }

                double? mostFreqFlowValue = null;
                if (thisRecord.Count(kvp => kvp.Key == "most_freq_flow_value") != 0)
                {
                    mostFreqFlowValue = double.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "most_freq_flow_value").Value);
                }

                int? mostFreqFlowNumber = null;
                if (thisRecord.Count(kvp => kvp.Key == "most_freq_flow_number") != 0)
                {
                    mostFreqFlowNumber = int.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "most_freq_flow_number").Value);
                }

                string characterizedDeviceValue = thisRecord.Count(kvp => kvp.Key == "characterized_device_value") != 0 ?
                                                    thisRecord.FirstOrDefault(kvp => kvp.Key == "characterized_device_value").Value : null;

                int? characterizedDeviceId = null;
                if (thisRecord.Count(kvp => kvp.Key == "characterized_device_id") != 0)
                {
                    characterizedDeviceId = int.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "characterized_device_id").Value);
                }

                var confirmedDeviceValue = thisRecord.Count(kvp => kvp.Key == "confirmed_device_value") != 0 ?
                                                thisRecord.FirstOrDefault(kvp => kvp.Key == "confirmed_device_value").Value : null;

                int? confirmedDeviceId = null;
                if (thisRecord.Count(kvp => kvp.Key == "confirmed_device_id") != 0)
                {
                    confirmedDeviceId = int.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "confirmed_device_id").Value);
                }

                var confirmationValue = thisRecord.Count(kvp => kvp.Key == "confirmation_value") != 0 ?
                                                thisRecord.FirstOrDefault(kvp => kvp.Key == "confirmation_value").Value : null;

                double? timeSinValue = null;
                if (thisRecord.Count(kvp => kvp.Key == "time_sin_value") != 0)
                {
                    timeSinValue = double.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "time_sin_value").Value);
                }

                double? timeCosValue = null;
                if (thisRecord.Count(kvp => kvp.Key == "time_cos_value") != 0)
                {
                    timeCosValue = double.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "time_cos_value").Value);
                }

                var consumptionCharacterizationData = new ConsumptionCharacterizationData
                {
                    MeterId = elementId,
                    From = @from,
                    To = to,
                    Volume = Volume,
                    Duration = duration,
                    Max_flow = maxFlow,
                    Most_freq_flow_value = mostFreqFlowValue,
                    Most_freq_flow_Number = mostFreqFlowNumber,
                    characterized_device_id = characterizedDeviceId,
                    characterized_device_value = characterizedDeviceValue,
                    confirmation = confirmationValue,
                    time_cos = timeCosValue,
                    time_sin = timeSinValue,
                    confirmed_device_id = confirmedDeviceId,
                    confirmed_device_value = confirmedDeviceValue
                };
                _currentContext.ConsumptionCharacterizationDatas.InsertOnSubmit(consumptionCharacterizationData);
            }

            _currentContext.SubmitChanges();
        }
        
        public void Update()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            
            foreach (var thisRecord in _currentRequestManager.Records)
            {
                var fromVar = Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "start").Value);

                float? Volume = null;
                if (thisRecord.Count(kvp => kvp.Key == "volume_value") != 0)
                {
                    Volume = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "volume_value").Value);
                }

                float? duration = null;
                if (thisRecord.Count(kvp => kvp.Key == "duration_value") != 0)
                {
                    duration = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "duration_value").Value);
                }

                float? maxFlow = null;
                if (thisRecord.Count(kvp => kvp.Key == "max_flow_value") != 0)
                {
                    maxFlow = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "max_flow_value").Value);
                }

                double? mostFreqFlowValue = null;
                if (thisRecord.Count(kvp => kvp.Key == "most_freq_flow_value") != 0)
                {
                    mostFreqFlowValue = double.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "most_freq_flow_value").Value);
                }

                int? mostFreqFlowNumber = null;
                if (thisRecord.Count(kvp => kvp.Key == "most_freq_flow_number") != 0)
                {
                    mostFreqFlowNumber = int.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "most_freq_flow_number").Value);
                }

                string characterizedDeviceValue = thisRecord.Count(kvp => kvp.Key == "characterized_device_value") != 0 ?
                                                    thisRecord.FirstOrDefault(kvp => kvp.Key == "characterized_device_value").Value : null;

                int? characterizedDeviceId = null;
                if (thisRecord.Count(kvp => kvp.Key == "characterized_device_id") != 0)
                {
                    characterizedDeviceId = int.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "characterized_device_id").Value);
                }

                var confirmedDeviceValue = thisRecord.Count(kvp => kvp.Key == "confirmed_device_value") != 0 ?
                                                thisRecord.FirstOrDefault(kvp => kvp.Key == "confirmed_device_value").Value : null;

                int? confirmedDeviceId = null;
                if (thisRecord.Count(kvp => kvp.Key == "confirmed_device_id") != 0)
                {
                    confirmedDeviceId = int.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "confirmed_device_id").Value);
                }

                var confirmationValue = thisRecord.Count(kvp => kvp.Key == "confirmation_value") != 0 ?
                                                thisRecord.FirstOrDefault(kvp => kvp.Key == "confirmation_value").Value : null;

                double? timeSinValue = null;
                if (thisRecord.Count(kvp => kvp.Key == "time_sin_value") != 0)
                {
                    timeSinValue = double.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "time_sin_value").Value);
                }

                double? timeCosValue = null;
                if (thisRecord.Count(kvp => kvp.Key == "time_cos_value") != 0)
                {
                    timeCosValue = double.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "time_cos_value").Value);
                }

                var record = _currentContext.ConsumptionCharacterizationDatas.FirstOrDefault(x => x.From == fromVar && x.characterized_device_value == characterizedDeviceValue);

                if (record != null)
                {
                    record.MeterId = elementId;

                    if (Volume != null)
                        record.Volume = Volume;
                    if (duration != null)
                        record.Duration = duration;
                    if (maxFlow != null)
                        record.Max_flow = maxFlow;
                    if (mostFreqFlowValue != null)
                        record.Most_freq_flow_value = mostFreqFlowValue;
                    if (mostFreqFlowNumber != null)
                        record.Most_freq_flow_Number = mostFreqFlowNumber;
                    if (characterizedDeviceId != null)
                        record.characterized_device_id = characterizedDeviceId;
                    if (!IsNullOrEmpty(confirmationValue))
                        record.confirmation = confirmationValue;
                    if (timeCosValue != null)
                        record.time_cos = timeCosValue;
                    if (timeSinValue != null)
                        record.time_sin = timeSinValue;
                    if (confirmedDeviceId != null)
                        record.confirmed_device_id = confirmedDeviceId;
                    if (!IsNullOrEmpty(confirmedDeviceValue))
                        record.confirmed_device_value = confirmedDeviceValue;
                    if (!IsNullOrEmpty(characterizedDeviceValue))
                        record.characterized_device_value = characterizedDeviceValue;
                    
                }

                _currentContext.SubmitChanges();
            }
        }
    }
}
