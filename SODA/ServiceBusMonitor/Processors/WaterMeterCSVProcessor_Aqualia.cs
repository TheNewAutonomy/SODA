using CsvHelper;
using DataAccess;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using static System.String;

namespace ServiceBusMonitor.Processor
{
    public class WaterMeterCSVProcessor_Aqualia : QueueProcessorBase, IQueueProcessor
    {
        public void ProcessQueue(CloudBlockBlob blob,
                                 CloudQueueMessage receivedMessage,
                                 CloudQueue urbanWaterQueue)
        {
            EventSourceWriter.Log.MessageMethod(
                $"WaterMeterCSVProcessor_Aqualia Message Processing Started. MessageId: {receivedMessage.Id}, BLOB name: {blob.Name}");

            var currentContext = new SQLAzureDataContext();

            var missingMeterIds = new List<string>();
            var processedMeterIds = new List<string>();

            using (var sr = new StringReader(blob.DownloadText()))
            {
                using (var csv = new CsvReader(sr))
                {
                    csv.Configuration.Delimiter = ";";
                    while (csv.Read())
                    {
                        try
                        {
                            if (csv.CurrentRecord[0] != null &&
                                csv.CurrentRecord[3] != null &&
                                csv.CurrentRecord[4] != null &&
                                csv.CurrentRecord[5] != null &&
                                CompareOrdinal(csv.CurrentRecord[5], "LITER") == 0)
                            {
                                var creationDateTime = DateTime.ParseExact(csv.CurrentRecord[3], "dd/MM/yyyy HH:mm:ss", null).ToUniversalTime();

                                var meterIdentity = csv.CurrentRecord[0];

                                if (meterIdentity.StartsWith("SAP-80-07-C"))
                                {
                                    meterIdentity = meterIdentity.Remove(0, 11);
                                }

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
                                        $"EXCEPTION: WaterMeterCSVProcessor_Aqualia. {e.Message}");
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
                                            Reading = csv.CurrentRecord[4],
                                            Encrypted = false,
                                            DMA = thisDma.Identifier
                                        };

                                        const string strWriteConnectionString = "MKWDNConnectionString";
                                        var blAttempt = WriteMessageMeterDataToDataTable(sm, strWriteConnectionString, thisDma.Site.TableName);

                                        if (!blAttempt)
                                        {
                                            EventSourceWriter.Log.MessageMethod(
                                                $"ERROR writing to BLOB storage by WaterMeterCSVProcessor_Aqualia. Received message Id: {receivedMessage.Id}BLOB name: {blob.Name}, PartitionKey: {sm.PartitionKey}");
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    EventSourceWriter.Log.MessageMethod(
                                        $"ERROR writing to BLOB storage in WaterMeterCSVProcessor_Aqualia. Received message Id: {receivedMessage.Id}BLOB name: {blob.Name}, Exception: {e.Message}");
                                }

                            }
                            else
                            {
                                EventSourceWriter.Log.MessageMethod(
                                    $"ERROR: Null values parsed in CSV file by WaterMeterCSVProcessor_Aqualia. Blob name: {blob.Name}");
                            }
                        }
                        catch (Exception e)
                        {
                            EventSourceWriter.Log.MessageMethod(
                                $"Exception processing CSV meter reading in WaterMeterCSVProcessor_Aqualia. Exception: {e.Message}");
                        }
                    }
                }
            }

            EventSourceWriter.Log.MessageMethod("WaterMeterCSVProcessor_Aqualia Message Processing Complete. MessageId:  " + receivedMessage.Id);

            urbanWaterQueue.DeleteMessage(receivedMessage);

            var newEvent = new Event
            {
                BusDispatched = false,
                EventDateTime = DateTime.Now,
                EventType = (int)EventTypes.NewMeteringData
            };

            var processedMetercsv = processedMeterIds.Aggregate(Empty, (current, meterId) => current + (meterId + ","));

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

                var metercsv = missingMeterIds.Aggregate(Empty, (current, meterId) => current + (meterId + ","));

                newMeterMissingEvent.Description = metercsv;

                currentContext.Events.InsertOnSubmit(newMeterMissingEvent);
            }

            currentContext.SubmitChanges();
        }
    }
}
