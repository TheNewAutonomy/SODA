using CsvHelper;
using DataAccess;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.IO;
using System.Linq;
using static System.Int32;

namespace ServiceBusMonitor.Processor
{
    public class WeatherQueueProcessor : QueueProcessorBase, IQueueProcessor
    {
        public void ProcessQueue(CloudBlockBlob blob,
                                 CloudQueueMessage receivedMessage,
                                 CloudQueue urbanWaterQueue)
        {
            EventSourceWriter.Log.MessageMethod("Worker Role Prcessing weather file " + receivedMessage.Id + " " + blob.Name);
            
            var currentContext = new SQLAzureDataContext();

            using (var sr = new StringReader(blob.DownloadText()))
            {
                using (var csv = new CsvReader(sr))
                {
                    while (csv.Read())
                    {
                        try
                        {
                            var newWeatherReading = new WeatherData
                            {
                                From = DateTime.Parse(csv.CurrentRecord[0]),
                                To = DateTime.Parse(csv.CurrentRecord[1]),
                                Humidity = Parse(csv.CurrentRecord[2]),
                                Precipitation = Parse(csv.CurrentRecord[3]),
                                Temperature = double.Parse(csv.CurrentRecord[4]),
                                Pressure = Parse(csv.CurrentRecord[5]),
                                Solar_radiation = Parse(csv.CurrentRecord[6]),
                                Wind_direction = csv.CurrentRecord[7],
                                Wind_velocity = double.Parse(csv.CurrentRecord[8]),
                                WeatherStation =
                                    currentContext.WeatherStations.FirstOrDefault(x => x.Name == csv.CurrentRecord[5])
                            };

                            currentContext.WeatherDatas.InsertOnSubmit(newWeatherReading);
                        }
                        catch (Exception e)
                        {
                            EventSourceWriter.Log.MessageMethod("Exception processing weather reading: " + e.Message);
                        }
                    }

                    currentContext.SubmitChanges();
                }
            }

            EventSourceWriter.Log.MessageMethod("Processing Complete Entry added to storage " + receivedMessage.Id);

            urbanWaterQueue.DeleteMessage(receivedMessage);

            var newEvent = new Event
            {
                BusDispatched = false,
                EventDateTime = DateTime.Now,
                EventType = (int) EventTypes.NewMeteringData
            };
            currentContext.Events.InsertOnSubmit(newEvent);
            currentContext.SubmitChanges();
        }
    }
}
