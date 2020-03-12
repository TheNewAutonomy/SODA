using CsvHelper;
using DataAccess;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TableDataAccess;

namespace ServiceBusMonitor
{
    public class WeatherQueueProcessor : QueueProcessorBase, IQueueProcessor
    {
        public void ProcessQueue(CloudBlockBlob blob,
                                 string strContainer,
                                 CloudQueueMessage receivedMessage,
                                 CloudQueue urbanWaterQueue)
        {
            EventSourceWriter.Log.MessageMethod("Worker Role Prcessing weather file " + receivedMessage.Id.ToString() + " " + blob.Name);
            
            SQLAzureDataContext currentContext = new SQLAzureDataContext();

            using (TextReader sr = new StringReader(blob.DownloadText()))
            {
                using (var csv = new CsvReader(sr))
                {
                    while (csv.Read())
                    {
                        try
                        {
                            WeatherData newWeatherReading = new WeatherData();
                            newWeatherReading.From = DateTime.Parse(csv.CurrentRecord[0]);
                            newWeatherReading.To = DateTime.Parse(csv.CurrentRecord[1]);
                            newWeatherReading.Humidity = Int32.Parse(csv.CurrentRecord[2]);
                            newWeatherReading.Precipitation = Int32.Parse(csv.CurrentRecord[3]);
                            newWeatherReading.Temperature = double.Parse(csv.CurrentRecord[4]);
                            newWeatherReading.Pressure = Int32.Parse(csv.CurrentRecord[5]);
                            newWeatherReading.Solar_radiation = Int32.Parse(csv.CurrentRecord[6]);
                            newWeatherReading.Wind_direction = csv.CurrentRecord[7];
                            newWeatherReading.Wind_velocity = double.Parse(csv.CurrentRecord[8]);
                            newWeatherReading.WeatherStation = currentContext.WeatherStations.FirstOrDefault(x => x.Name == csv.CurrentRecord[5]);

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

            EventSourceWriter.Log.MessageMethod("Processing Complete Entry added to storage " + receivedMessage.Id.ToString());

            urbanWaterQueue.DeleteMessage(receivedMessage);

            Event newEvent = new Event();
            newEvent.BusDispatched = false;
            newEvent.EventDateTime = DateTime.Now;
            newEvent.EventType = (int)EventTypes.NewMeteringData;
            currentContext.Events.InsertOnSubmit(newEvent);
            currentContext.SubmitChanges();
        }
    }
}
