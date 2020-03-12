using DataAccess;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RabbitMQConnector
{
    public class DateTimeBlock
    {
        public DateTimeOffset From { get; set; }

        public DateTimeOffset To { get; set; }
    }

    public class WeatherDataManager
    {
        readonly RequestManager _currentRequestManager;
        SQLAzureDataContext _currentContext;

        public WeatherDataManager(RequestManager requestManager)
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
            long timestep   = _currentRequestManager.RootElements.Count(kvp => kvp.Key == "timeStep") > 0 ?
                              int.Parse(_currentRequestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "timeStep").Value) : -1;

            var resultTxt = new StringBuilder();
            var allResults = _currentContext.GetWeatherData(start, end, elementId).ToList();

            if (allResults.Any())
            {
      //          start = allResults.Aggregate((acc, c) => acc.From < c.From ? acc : c).From;
      //          end = allResults.Aggregate((acc, c) => acc.To > c.To ? acc : c).To;
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

                    var subResults = new List<GetWeatherDataResult>();

                    // loop through each block and and find any records that span that block, trimming those on the edges
                    foreach (var thisBlock in dateTimeBlockSet)
                    {
                        var firstRecordkInSet =
                            allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From)
                                .OrderBy(x => x.From)
                                .FirstOrDefault();
                        var lastRecordInSet =
                            allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From)
                                .OrderBy(x => x.From)
                                .Reverse()
                                .LastOrDefault();

                        if (firstRecordkInSet != null && lastRecordInSet != null)
                        {
                            var recordsSpanningBlock =
                                allResults.Where(x => x.From >= firstRecordkInSet.From && x.To <= lastRecordInSet.To);

                            var getWeatherDataResults = recordsSpanningBlock as GetWeatherDataResult[] ??
                                                        recordsSpanningBlock.ToArray();
                            if (getWeatherDataResults.Any())
                            {
                                var trimmedrecordsSpanningBlock = new List<GetWeatherDataResult>();
                                foreach (var record in getWeatherDataResults)
                                {
                                    double percentageToTrim = 0;

                                    if (record.From < thisBlock.From)
                                    {
                                        long currentBlockTicks = record.To.Ticks - record.From.Ticks;
                                        long ticksToTrim = thisBlock.From.Ticks - record.From.Ticks;

                                        percentageToTrim +=
                                            (double) (((double) 100.0/(double) currentBlockTicks)*(double) ticksToTrim);
                                    }
                                    if (record.To > thisBlock.To)
                                    {
                                        long currentBlockTicks = record.To.Ticks - record.From.Ticks;
                                        long ticksToTrim = record.To.Ticks - thisBlock.To.Ticks;

                                        percentageToTrim =
                                            (double) (((double) 100.0/(double) currentBlockTicks)*(double) ticksToTrim);
                                    }

                                    if (record.Precipitation != null)
                                    {
                                        record.Precipitation = record.Precipitation -
                                                               ((record.Precipitation/100)*percentageToTrim);
                                    }
                                    trimmedrecordsSpanningBlock.Add(record);
                                }

                                int? humidity = null;
                                double? precipitation = null;
                                int? pressure = null;
                                double? solarRadiation = null;
                                double? temperature = null;
                                string windDirection = null;
                                double? windVelocity = null;

                                foreach (var reading in trimmedrecordsSpanningBlock)
                                {
                                    if (reading.Humidity != null)
                                    {
                                        if (humidity == null)
                                        {
                                            humidity = reading.Humidity;
                                        }
                                        else
                                        {
                                            humidity += reading.Humidity;
                                        }
                                    }
                                    if (reading.Precipitation != null)
                                    {
                                        if (precipitation == null)
                                        {
                                            precipitation = reading.Precipitation;
                                        }
                                        else
                                        {
                                            precipitation += reading.Precipitation;
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
                                    if (reading.Solar_radiation != null)
                                    {
                                        if (solarRadiation == null)
                                        {
                                            solarRadiation = reading.Solar_radiation;
                                        }
                                        else
                                        {
                                            solarRadiation += reading.Solar_radiation;
                                        }
                                    }
                                    if (reading.Temperature != null)
                                    {
                                        if (temperature == null)
                                        {
                                            temperature = reading.Temperature;
                                        }
                                        else
                                        {
                                            temperature += reading.Temperature;
                                        }
                                    }
                                    windDirection = reading.Wind_direction;
                                    if (reading.Wind_velocity != null)
                                    {
                                        if (windVelocity == null)
                                        {
                                            windVelocity = reading.Wind_velocity;
                                        }
                                        else
                                        {
                                            windVelocity += reading.Wind_velocity;
                                        }
                                    }
                                }

                                var modifiedReading = trimmedrecordsSpanningBlock.FirstOrDefault();

                                if (humidity != null)
                                    modifiedReading.Humidity = humidity/trimmedrecordsSpanningBlock.Count();
                                if (precipitation != null)
                                    modifiedReading.Precipitation = precipitation;
                                if (pressure != null)
                                    modifiedReading.Pressure = pressure/trimmedrecordsSpanningBlock.Count();
                                if (solarRadiation != null)
                                    modifiedReading.Solar_radiation = solarRadiation/trimmedrecordsSpanningBlock.Count();
                                if (temperature != null)
                                    modifiedReading.Temperature = temperature/trimmedrecordsSpanningBlock.Count();
                                modifiedReading.Wind_direction = windDirection;
                                if (windVelocity != null)
                                    modifiedReading.Wind_velocity = windVelocity/trimmedrecordsSpanningBlock.Count();

                                modifiedReading.From = thisBlock.From;
                                modifiedReading.To = thisBlock.To;

                                subResults.Add(modifiedReading);
                            }
                        }
                    }

                    allResults = subResults;
                }

                foreach (var thisReading in allResults)
                {
                    var precipitationValue = string.Empty;
                    var temperatureValue = string.Empty;
                    var humidityValue = string.Empty;
                    var pressureValue = string.Empty;
                    var windVelocityValue = string.Empty;
                    var windDirectionValue = string.Empty;
                    var solarRadiationValue = string.Empty;

                    if (indices.Contains(2) && thisReading.Precipitation != null)
                    {
                        precipitationValue = "<variable name=\"precipitation\">" +
                                             $"<value>{thisReading.Precipitation}</value></variable>";
                    }
                    if (indices.Contains(3) && thisReading.Temperature != null)
                    {
                        temperatureValue = "<variable name=\"temperature\">" +
                                           $"<value>{thisReading.Temperature}</value></variable>";
                    }
                    if (indices.Contains(4) && thisReading.Humidity != null)
                    {
                        humidityValue = "<variable name=\"humidity\">" +
                                        $"<value>{thisReading.Humidity}</value></variable>";
                    }
                    if (indices.Contains(5) && thisReading.Pressure != null)
                    {
                        pressureValue = "<variable name=\"pressure\">" +
                                        $"<value>{thisReading.Pressure}</value></variable>";
                    }
                    if (indices.Contains(6) && thisReading.Wind_velocity != null)
                    {
                        windVelocityValue = "<variable name=\"wind_velocity\">" +
                                            $"<value>{thisReading.Wind_velocity}</value></variable>";
                    }
                    if (indices.Contains(7) && thisReading.Wind_direction != null)
                    {
                        windDirectionValue = "<variable name=\"wind_direction\">" +
                                             $"<value>{thisReading.Wind_direction}</value></variable>";
                    }
                    if (indices.Contains(8) && thisReading.Solar_radiation != null)
                    {
                        solarRadiationValue = "<variable name=\"solar_radiation\">" +
                                              $"<value>{thisReading.Solar_radiation}</value></variable>";
                    }

                    if (!string.IsNullOrEmpty(precipitationValue) || !string.IsNullOrEmpty(temperatureValue) ||
                        !string.IsNullOrEmpty(humidityValue) || !string.IsNullOrEmpty(pressureValue) ||
                        !string.IsNullOrEmpty(windVelocityValue) || !string.IsNullOrEmpty(windDirectionValue) ||
                        !string.IsNullOrEmpty(solarRadiationValue))
                    {
                        resultTxt.Append("<record>" + $"<from>{thisReading?.From}</from>" +
                                     $"<to>{thisReading?.To}</to>{precipitationValue}{temperatureValue}{humidityValue}{pressureValue}{windDirectionValue}{windVelocityValue}{solarRadiationValue}</record>{Environment.NewLine}");
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

            var allResults = _currentContext.GetWeatherData(start, end, elementId).ToList();
            
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

                var subResults = new List<GetWeatherDataResult>();

                // loop through each block and and find any records that span that block, trimming those on the edges
                foreach (var thisBlock in dateTimeBlockSet)
                {
                    var firstRecordkInSet = allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From).OrderBy(x => x.From).FirstOrDefault();
                    var lastRecordInSet = allResults.Where(x => x.From < thisBlock.To && x.To > thisBlock.From).OrderBy(x => x.From).Reverse().LastOrDefault();

                    if (firstRecordkInSet != null && lastRecordInSet != null)
                    {
                        var recordsSpanningBlock = allResults.Where(x => x.From >= firstRecordkInSet.From && x.To <= lastRecordInSet.To);

                        var getWeatherDataResults = recordsSpanningBlock as GetWeatherDataResult[] ?? recordsSpanningBlock.ToArray();
                        if (getWeatherDataResults.Any())
                        {
                            var trimmedrecordsSpanningBlock = new List<GetWeatherDataResult>();
                            foreach (var record in getWeatherDataResults)
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

                                if (record.Precipitation != null)
                                {
                                    record.Precipitation = record.Precipitation - ((record.Precipitation / 100) * percentageToTrim);
                                }
                                trimmedrecordsSpanningBlock.Add(record);
                            }

                            int? humidity = null;
                            double? precipitation = null;
                            int? pressure = null;
                            double? solarRadiation = null;
                            double? temperature = null;
                            string windDirection = null;
                            double? windVelocity = null;

                            foreach (var reading in trimmedrecordsSpanningBlock)
                            {
                                if (reading.Humidity != null)
                                {
                                    if (humidity == null)
                                    {
                                        humidity = reading.Humidity;
                                    }
                                    else
                                    {
                                        humidity += reading.Humidity;
                                    }
                                }
                                if (reading.Precipitation != null)
                                {
                                    if (precipitation == null)
                                    {
                                        precipitation = reading.Precipitation;
                                    }
                                    else
                                    {
                                        precipitation += reading.Precipitation;
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
                                if (reading.Solar_radiation != null)
                                {
                                    if (solarRadiation == null)
                                    {
                                        solarRadiation = reading.Solar_radiation;
                                    }
                                    else
                                    {
                                        solarRadiation += reading.Solar_radiation;
                                    }
                                }
                                if (reading.Temperature != null)
                                {
                                    if (temperature == null)
                                    {
                                        temperature = reading.Temperature;
                                    }
                                    else
                                    {
                                        temperature += reading.Temperature;
                                    }
                                }
                                windDirection = reading.Wind_direction;
                                if (reading.Wind_velocity != null)
                                {
                                    if (windVelocity == null)
                                    {
                                        windVelocity = reading.Wind_velocity;
                                    }
                                    else
                                    {
                                        windVelocity += reading.Wind_velocity;
                                    }
                                }
                            }

                            var modifiedReading = trimmedrecordsSpanningBlock.FirstOrDefault();

                            if (humidity != null)
                                modifiedReading.Humidity = humidity / trimmedrecordsSpanningBlock.Count();
                            if (precipitation != null)
                                modifiedReading.Precipitation = precipitation;
                            if (pressure != null)
                                modifiedReading.Pressure = pressure / trimmedrecordsSpanningBlock.Count();
                            if (solarRadiation != null)
                                modifiedReading.Solar_radiation = solarRadiation / trimmedrecordsSpanningBlock.Count();
                            if (temperature != null)
                                modifiedReading.Temperature = temperature / trimmedrecordsSpanningBlock.Count();
                            modifiedReading.Wind_direction = windDirection;
                            if (windVelocity != null)
                                modifiedReading.Wind_velocity = windVelocity / trimmedrecordsSpanningBlock.Count();

                            modifiedReading.From = thisBlock.From;
                            modifiedReading.To = thisBlock.To;

                            subResults.Add(modifiedReading);
                        }
                    }
                }

                allResults = subResults;
            }

            var resultTxt = new StringBuilder();

            foreach (var thisReading in allResults)
            {
                var precipitationValue = thisReading.Precipitation != null ?
                    "<variable name=\"precipitation\">" + $"<value>{thisReading.Precipitation}</value>" + "</variable>"
                    : null;

                var temperatureValue = thisReading.Temperature != null ?
                    "<variable name=\"temperature\">" + $"<value>{thisReading.Temperature}</value>" + "</variable>"
                    : null;

                var humidityValue = thisReading.Humidity != null ?
                    "<variable name=\"humidity\">" + $"<value>{thisReading.Humidity}</value>" + "</variable>"
                    : null;

                var PressureValue = thisReading.Pressure != null ?
                    "<variable name=\"pressure\">" + $"<value>{thisReading.Pressure}</value>" + "</variable>"
                    : null;

                var Wind_DirectionValue = thisReading.Wind_direction != null ?
                    "<variable name=\"wind_direction\">" + $"<value>{thisReading.Wind_direction}</value>" +
                    "</variable>"
                    : null;

                var Wind_VelocityValue = thisReading.Wind_velocity != null ?
                    "<variable name=\"wind_velocity\">" + $"<value>{thisReading.Wind_velocity}</value>" + "</variable>"
                    : null;

                var Solar_radiationValue = thisReading.Solar_radiation != null ?
                    "<variable name=\"solar_radiation\">" + $"<value>{thisReading.Solar_radiation}</value>" +
                    "</variable>"
                    : null;
               
                resultTxt.Append("<record>" + $"<from>{thisReading.From.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</from>" +
                             $"<to>{thisReading.To.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ssK")}</to>{precipitationValue}{temperatureValue}{humidityValue}{PressureValue}{Wind_DirectionValue}{Wind_VelocityValue}{Solar_radiationValue}</record>{Environment.NewLine}");
            }
            
            return "<response><recordSet>" +
                              $"<elementId>{elementId}</elementId>{resultTxt}</recordSet>" + "</response>";
        }

        public void Create()
        {
            _currentContext = new SQLAzureDataContext();

            foreach (var thisRecord in _currentRequestManager.Records)
            {
                var elementId = thisRecord.FirstOrDefault(kvp => kvp.Key == "elementId").Value;
                var from      = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "from").Value);
                var to        = DateTimeOffset.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "to").Value);

                double? precipitation = null;
                if (thisRecord.Count(kvp => kvp.Key == "precipitation_value") != 0)
                {
                    precipitation = double.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "precipitation_value").Value);
                }

                double? temperature = null;
                if (thisRecord.Count(kvp => kvp.Key == "temperature_value") != 0)
                {
                    temperature = double.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "temperature_value").Value);
                }

                int? humidity = null;
                if (thisRecord.Count(kvp => kvp.Key == "humidity_value") != 0)
                {
                    humidity = int.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "humidity_value").Value);
                }

                int? pressure = null;
                if (thisRecord.Count(kvp => kvp.Key == "pressure_value") != 0)
                {
                    pressure = int.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "pressure_value").Value);
                }

                double? windVelocity = null;
                if (thisRecord.Count(kvp => kvp.Key == "wind_velocity_value") != 0)
                {
                    windVelocity = double.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "wind_velocity_value").Value);
                }

                var windDirection = thisRecord.Count(kvp => kvp.Key == "wind_direction_value") != 0 ?
                                      thisRecord.FirstOrDefault(kvp => kvp.Key == "wind_direction_value").Value : null;

                double? solarRadiation = null;
                if (thisRecord.Count(kvp => kvp.Key == "solar_radiation_value") != 0)
                {
                    solarRadiation = double.Parse(thisRecord.FirstOrDefault(kvp => kvp.Key == "solar_radiation_value").Value);
                }

                _currentContext.InsertWeatherData(elementId, from, to, precipitation, temperature, humidity, pressure,
                    windVelocity, windDirection, solarRadiation);
            }

            _currentContext.SubmitChanges();
        }
    }
}
