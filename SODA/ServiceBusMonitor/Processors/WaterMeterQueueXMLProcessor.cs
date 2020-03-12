using DataAccess;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace ServiceBusMonitor.Processor
{
    public class WaterMeterQueueXMLProcessor : QueueProcessorBase, IQueueProcessor
    {
        public void ProcessQueue(CloudBlockBlob blob,
                                 CloudQueueMessage receivedMessage,
                                 CloudQueue urbanWaterQueue)
        {
            var currentContext = new SQLAzureDataContext();

            var xml = blob.DownloadText();
            var xDoc = XDocument.Parse(xml);

            var root = xDoc.Elements();
            var gateways = root.Elements().FirstOrDefault(x => x.Name.LocalName == "gateways");

            var missingMeterIds = new List<string>();
            var processedMeterIds = new List<string>();

            try
            {
                foreach (var gateway in gateways?.Elements().Where(x => x.Name.LocalName == "gateway"))
                {
                    var collections = gateway.Elements().FirstOrDefault(x => x.Name.LocalName == "collections");
                    foreach (var collection in collections.Elements().Where(x => x.Name.LocalName == "collection"))
                    {
                        var producerId = collection.Elements().FirstOrDefault(x => x.Name.LocalName == "producer-id").Value;
                        var data = collection.Elements().FirstOrDefault(x => x.Name.LocalName == "data");

                        foreach (var record in data.Elements())
                        {
                            var recordDateTime = record.Attributes().FirstOrDefault(x => x.Name.LocalName == "ts").Value;
                            var recordValue = record.Elements().FirstOrDefault(x => x.Name.LocalName == "CURRENT_INDEX").Value;

                            if (!string.IsNullOrEmpty(producerId) &&
                                !string.IsNullOrEmpty(recordDateTime) &&
                                !string.IsNullOrEmpty(recordValue)
                                )
                            {
                                try
                                {
                                    var meterSet = currentContext.Meters.Where(x => x.MeterIdentity == producerId);

                                    if (!meterSet.Any())
                                    {
                                        if (!missingMeterIds.Contains(producerId))
                                        {
                                            missingMeterIds.Add(producerId);
                                        }
                                    }
                                    else
                                    {
                                        if (!processedMeterIds.Contains(producerId))
                                        {
                                            processedMeterIds.Add(producerId);
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    EventSourceWriter.Log.MessageMethod("EXCEPTION: WaterMeterQueueXMLProcessor. " + e.Message);
                                }

                                try
                                {
                                    var firstOrDefault = currentContext.Meters.FirstOrDefault(x => x.MeterIdentity == producerId);
                                    if (firstOrDefault != null)
                                    {
                                        var thisDma =
                                            firstOrDefault.DMA;

                                        var sm = new MeterReadingEntity
                                        {
                                            PartitionKey = producerId,
                                            CreatedOn = DateTime.Parse(recordDateTime),
                                            RowKey = DateTime.Parse(recordDateTime).Ticks.ToString(),
                                            Reading = recordValue,
                                            Encrypted = false,
                                            DMA = thisDma.Identifier
                                        };

                                        const string strWriteConnectionString = "MKWDNConnectionString";
                                        var blAttempt = WriteMessageMeterDataToDataTable(sm,
                                            strWriteConnectionString, thisDma.Site.TableName);

                                        if (!blAttempt)
                                        {
                                            EventSourceWriter.Log.MessageMethod(
                                                $"ERROR writing to BLOB storage by WaterMeterQueueXMLProcessor. Received message Id: {receivedMessage.Id}BLOB name: {blob.Name}, PartitionKey: {sm.PartitionKey}");
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    EventSourceWriter.Log.MessageMethod(
                                        $"ERROR writing to BLOB storage in WaterMeterQueueXMLProcessor. Received message Id: {receivedMessage.Id}BLOB name: {blob.Name}, Exception: {e.Message}");
                                }

                            }
                            else
                            {
                                EventSourceWriter.Log.MessageMethod(
                                    $"ERROR: Null values parsed in CSV file by WaterMeterQueueXMLProcessor. Blob name: {blob.Name}");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                EventSourceWriter.Log.MessageMethod(
                    $"Exception processing CSV meter reading in WaterMeterQueueXMLProcessor. Exception: {e.Message}");
            }
            
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
