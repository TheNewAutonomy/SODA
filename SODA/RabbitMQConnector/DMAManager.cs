using DataAccess;
using System;
using System.Linq;

namespace RabbitMQConnector
{
    public class DMAManager
    {
        readonly RequestManager _currentRequestManager;
        SQLAzureDataContext _currentContext;

        public DMAManager(RequestManager requestManager)
        {
            _currentRequestManager = requestManager;
        }

        public string ReadList()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.First(kvp => kvp.Key == "elementId");
            var dmAs      = _currentContext.DMAs.Where(x => x.Site.Name == elementId.Value);

            var resultTxt = string.Empty;
            foreach (var thisReading in dmAs)
            {
                resultTxt += "<dma>" + $"<elementId>{thisReading.Identifier}</elementId>" +
                             $"<name>{thisReading.Name}</name>" +
                             $"<weatherStationId>{thisReading.WeatherStation.Name}</weatherStationId>" +
                             $"<burstThreshold>{thisReading.BurstThreshold}</burstThreshold>" +
                             $"</dma>{Environment.NewLine}";
            }

            var response = "<response>" + "<recordSet>" +
                           $"<elementId>{elementId.Value}</elementId>{resultTxt}</recordSet>" + "</response>";

            return response;
        }

        public string Read()
        {
            _currentContext = new SQLAzureDataContext();

            var elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var dmas      = _currentContext.DMAs.Where(x => x.Identifier == elementId);
            var resultTxt = string.Empty;

            foreach (var thisReading in dmas)
            {
                resultTxt += "<dma>" + $"<weatherStationId>{thisReading.WeatherStation.Name}</weatherStationId>" +
                             $"<burstThreshold>{thisReading.BurstThreshold}</burstThreshold>" +
                             $"</dma>{Environment.NewLine}";
            }

            var dmaName = string.Empty;
            if (resultTxt != string.Empty)
            {
                var firstOrDefault = _currentContext.DMAs.FirstOrDefault(x => x.Identifier == elementId);
                if (firstOrDefault != null)
                    dmaName = firstOrDefault.Name;
            }
            var response = "<response>" + "<dmaSet>" + $"<elementId>{elementId}</elementId>" +
                           $"<name>{dmaName}</name>{resultTxt}</dmaSet>" + "</response>";

            return response;
        }
    }
}
