using CsvHelper;
using DataAccess;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ServiceBusMonitor.Processor
{
    public class WaterMeterCSVProcessor_Ovod : QueueProcessorBase, IQueueProcessor
    {
        public void ProcessQueue(CloudBlockBlob blob,
                                 CloudQueueMessage receivedMessage,
                                 CloudQueue urbanWaterQueue)
        {
            EventSourceWriter.Log.MessageMethod(
                $"WaterMeterCSVProcessor_Ovod Message Processing Started. MessageId: {receivedMessage.Id}, BLOB name: {blob.Name}");

            var currentContext = new SQLAzureDataContext();

            var missingMeterIds = new List<string>();
            var processedMeterIds = new List<string>();

            using (var sr = new StringReader(blob.DownloadText()))
            {
                using (var csv = new CsvReader(sr))
                {
                    csv.Configuration.Delimiter = ",";
                    while (csv.Read())
                    {
                        try
                        {
                            if (csv.CurrentRecord[1] != null &&
                                csv.CurrentRecord[2] != null &&
                                csv.CurrentRecord[3] != null &&
                                csv.CurrentRecord[0] != null)
                            {
                                var creationDateTime = DateTime.ParseExact(csv.CurrentRecord[2], "MM/dd/yyyy hh:mm tt", null).ToUniversalTime();

                                var meterIdentity = csv.CurrentRecord[1];

                                try
                                {
                                    var meterSet = currentContext.Meters.Where(x => x.MeterIdentity == meterIdentity);

                                    if (!meterSet.Any())
                                    {
                                        if (!missingMeterIds.Contains(meterIdentity))
                                        {
                                            missingMeterIds.Add(meterIdentity);
                                        }
                                    }
                                    else
                                    {
                                        if (!processedMeterIds.Contains(meterIdentity))
                                        {
                                            processedMeterIds.Add(meterIdentity);
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    EventSourceWriter.Log.MessageMethod(
                                        $"EXCEPTION: WaterMeterCSVProcessor_Ovod. {e.Message}");
                                }

                                try
                                {
                                    var firstOrDefault = currentContext.Meters.FirstOrDefault(x => x.MeterIdentity == meterIdentity);
                                    if (firstOrDefault != null)
                                    {
                                        var thisDma =
                                            firstOrDefault.DMA;

                                        var sm = new MeterReadingEntity
                                        {
                                            PartitionKey = meterIdentity,
                                            CreatedOn = creationDateTime,
                                            RowKey = creationDateTime.Ticks.ToString(),
                                            Reading = csv.CurrentRecord[3],
                                            Encrypted = false,
                                            DMA = thisDma.Identifier
                                        };

                                        const string strWriteConnectionString = "MKWDNConnectionString";
                                        var blAttempt = WriteMessageMeterDataToDataTable(sm,
                                            strWriteConnectionString, thisDma.Site.TableName);

                                        if (!blAttempt)
                                        {
                                            EventSourceWriter.Log.MessageMethod(
                                                $"ERROR writing to BLOB storage by WaterMeterCSVProcessor_Ovod. Received message Id: {receivedMessage.Id}BLOB name: {blob.Name}, PartitionKey: {sm.PartitionKey}");
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    EventSourceWriter.Log.MessageMethod(
                                        $"ERROR writing to BLOB storage in WaterMeterCSVProcessor_Ovod. Received message Id: {receivedMessage.Id}BLOB name: {blob.Name}, Exception: {e.Message}");
                                }

                            }
                            else
                            {
                                EventSourceWriter.Log.MessageMethod(
                                    $"ERROR: Null values parsed in CSV file by WaterMeterCSVProcessor_Ovod. Blob name: {blob.Name}");
                            }
                        }
                        catch (Exception e)
                        {
                            EventSourceWriter.Log.MessageMethod(
                                $"Exception processing CSV meter reading in WaterMeterCSVProcessor_Ovod. Exception: {e.Message}");
                        }
                    }
                }
            }

            EventSourceWriter.Log.MessageMethod(
                $"WaterMeterCSVProcessor_Ovod Message Processing Complete. MessageId:  {receivedMessage.Id}");

            urbanWaterQueue.DeleteMessage(receivedMessage);

            var newEvent = new Event
            {
                BusDispatched = false,
                EventDateTime = DateTime.Now,
                EventType = (int)EventTypes.NewMeteringData
            };

            var processedMetercsv = processedMeterIds.Aggregate(string.Empty, (current, meterId) => current + (meterId + ", "));

            newEvent.Description = processedMetercsv;

            currentContext.Events.InsertOnSubmit(newEvent);

            if (missingMeterIds.Count > 0)
            {
                var newMeterMissingEvent = new Event
                {
                    BusDispatched = false,
                    EventDateTime = DateTime.Now,
                    EventType = (int)EventTypes.MissingMeter
                };

                var metercsv = missingMeterIds.Aggregate(string.Empty, (current, meterId) => current + (meterId + ", "));

                newMeterMissingEvent.Description = metercsv;

                currentContext.Events.InsertOnSubmit(newMeterMissingEvent);
            }

            currentContext.SubmitChanges();
        }
    }
}
