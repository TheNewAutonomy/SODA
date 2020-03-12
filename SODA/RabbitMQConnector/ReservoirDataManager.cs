using DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RabbitMQConnector
{
    public class ReservoirDataManager
    {
        readonly RequestManager _currentRequestManager;
        SQLAzureDataContext _currentContext;

        public ReservoirDataManager(RequestManager requestManager)
        {
            _currentRequestManager = requestManager;
        }

        public string Read()
        {
            _currentContext = new SQLAzureDataContext();

            var   elementId = _currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
            var start       = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "start").Value);
            var end         = DateTimeOffset.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "end").Value);
            long timestep   = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "timeStep") > 0 ?
                              int.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;

            var allResults = _currentContext.ReservoirDatas.Where(x => x.Reservoir.Identifier == elementId && x.From >= start && x.To <= end).OrderBy(x => x.From).ToList();
            if (timestep != -1 && allResults.Any())
            {
                var subResults = new List<ReservoirData>();

                const long multiplier = 10000000;
                long timestepTicks = timestep * multiplier;

                long startTick = allResults.FirstOrDefault().From.Ticks;
                long endTick = allResults.LastOrDefault().From.Ticks;
                long counter = startTick;

                while (counter <= endTick)
                {
                    // var TimeStepResults = allResults.OrderBy(item => Math.Abs(counter - item.CreationTime.Ticks)).First();
                    var timeStepResults = allResults.Aggregate((x, y) => Math.Abs(x.From.Ticks - counter) < Math.Abs(y.From.Ticks - counter) ? x : y);

                    if (timeStepResults != null)
                    {
                        counter += timestepTicks;
                        subResults.Add(timeStepResults);
                    }
                }

                allResults = subResults.GroupBy(x => x.ReservoirId).Select(y => y.First()).ToList();
            }

            var resultTxt = new StringBuilder();

            foreach (var thisReading in allResults)
            {
                var volumeValue = thisReading.Volume != null ?
                    "<variable name=\"volume\">" + $"<value>{thisReading.Volume}</value>" + "</variable>"
                    : null;

                var levelValue = thisReading.Level != null ?
                    "<variable name=\"level\">" + $"<value>{thisReading.Level}</value>" + "</variable>"
                    : null;
                
                resultTxt.Append("<record>" + $"<from>{thisReading?.From.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</from>" +
                             $"<to>{thisReading?.To.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</to>{volumeValue}{levelValue}</record>{Environment.NewLine}");
            }

            var reservoirName = string.Empty;
            if (resultTxt.Length > 0)
            {
                reservoirName = _currentContext.Reservoirs.FirstOrDefault(x => x.Identifier == elementId)?.Name;
            }
            return "<response><recordSet>" + $"<elementId>{elementId}</elementId>" +
                           $"<name>{reservoirName}</name>{resultTxt}</recordSet>" + "</response>";
        }

        public void Create()
        {
            _currentContext = new SQLAzureDataContext();

            foreach (var thisRecord in _currentRequestManager.Records)
            {
                var elementId = thisRecord.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
                var from      = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "from").Value);
                var to        = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "to").Value);
                float? volume = null;
                if (thisRecord.Count(kvp => kvp.Key == "volume_value") != 0)
                {
                    volume = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "volume_value").Value);
                }

                float? level = null;
                if (thisRecord.Count(kvp => kvp.Key == "level_value") != 0)
                {
                    level = float.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "level_value").Value);
                }

                var reservoirData = new ReservoirData
                {
                    Reservoir = _currentContext.Reservoirs.FirstOrDefault(x => x.Identifier == elementId),
                    From = from,
                    To = to,
                    Volume = volume,
                    Level = level
                };

                _currentContext.ReservoirDatas.InsertOnSubmit(reservoirData);
            }

            _currentContext.SubmitChanges();
        }
    }
}
