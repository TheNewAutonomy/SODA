using DataAccess;
using System;
using System.Linq;

namespace RabbitMQConnector
{
    class Monitoring
    {
        private readonly SQLAzureDataContext _currentContext = new SQLAzureDataContext();

        public enum EventTypes { NewMeteringData = 1, ServiceStart = 2, MissingMeter = 3 };

        public string GetServiceStartDateTime()
        {
            var lastStartEvent = _currentContext.Events.FirstOrDefault(x => x.EventType == (int)EventTypes.ServiceStart);

            var lastStart = lastStartEvent.EventDateTime;
            var difference = DateTime.Now - lastStart;

            var response = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n\t" +
                           "<ServiceInformation>\n\t" + "<ID>uw.service.cds</ID>\n\t" +
                           "<Name>CloudDataService</Name>\n\t" + "<version>0.0.2</version>\n\t" +
                           $"<Uptime>{difference.TotalMilliseconds}</Uptime>\n\t" +
                           $"<LocalTime>{DateTime.Now.ToString("O")}</LocalTime>\n\t" +
                           "<UsageOfCPU>0.5</UsageOfCPU>\n\t" + "<UsageOfRam>56</UsageOfRam>\n\t" +
                           "<NetworkRxBytes>1932048309</NetworkRxBytes>\n\t" +
                           "<NetworkTxBytes>217129403</NetworkTxBytes>\n\t" + "</ServiceInformation>\n\t";
            
            return response;
        }

        public void SetStartDateTime()
        {
            var newEvent = new Event
            {
                EventDateTime = DateTime.Now,
                EventType = (int) EventTypes.ServiceStart,
                BusDispatched = false
            };

            _currentContext.Events.InsertOnSubmit(newEvent);
            _currentContext.SubmitChanges();
        }

        public string CheckMeteringDispatch()
        {
            var strRet = string.Empty;
            var meteringEvent = _currentContext.Events.Where(x => x.EventType == (int)EventTypes.NewMeteringData && x.BusDispatched == false);

            if (meteringEvent.Any())
            {
                foreach (var thisMeteringEvent in meteringEvent)
                {
                    strRet += thisMeteringEvent.Description.Trim();
                    thisMeteringEvent.BusDispatched = true;
                }

                strRet = strRet.Trim().TrimEnd(',');

                var meterIds = strRet.Split(',').Distinct().ToList();

                strRet = string.Empty;

                foreach (string word in meterIds)
                {
                    strRet += word + ",";
                }

                strRet = strRet.Trim().TrimEnd(',');

                _currentContext.SubmitChanges();
            }

            return strRet;
        }

        public string CheckMissingMeterIdsDispatch()
        {
            var strRet = string.Empty;
            var meteringEvent = _currentContext.Events.Where(x => x.EventType == (int)EventTypes.MissingMeter && x.BusDispatched == false);
            if (meteringEvent.Any())
            {
                foreach (var thisMeteringEvent in meteringEvent)
                {
                    strRet += thisMeteringEvent.Description;
                    thisMeteringEvent.BusDispatched = true;
                }

                strRet = strRet.Trim().TrimEnd(',');

                var meterIds = strRet.Split(',').Distinct().ToList();

                strRet = string.Empty;

                foreach (string word in meterIds)
                {
                    strRet += word + ",";
                }

                strRet = strRet.Trim().TrimEnd(',');

                _currentContext.SubmitChanges();
            }
            return strRet;
        }
    }
}
