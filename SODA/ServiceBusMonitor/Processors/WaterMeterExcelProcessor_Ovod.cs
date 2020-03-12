using DataAccess;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;

namespace ServiceBusMonitor.Processor
{
    public class WaterMeterExcelProcessor_Ovod : QueueProcessorBase, IQueueProcessor
    {
        public void ProcessQueue(CloudBlockBlob blob,
                                 CloudQueueMessage receivedMessage,
                                 CloudQueue urbanWaterQueue)
        {
            EventSourceWriter.Log.MessageMethod(
                $"WaterMeterExcelProcessor_Ovod Message Processing Started. MessageId: {receivedMessage.Id}, BLOB name: {blob.Name}");

            var currentContext = new SQLAzureDataContext();

            var missingMeterIds = new List<string>();
            var processedMeterIds = new List<string>();

            var stream = new MemoryStream();
            blob.DownloadToStream(stream);

            using (var spreadsheetDocument = SpreadsheetDocument.Open(stream, false))
            {
                var workbookPart = spreadsheetDocument.WorkbookPart;
                var worksheetPart = workbookPart.WorksheetParts.First();
                var sheetData = worksheetPart.Worksheet.Elements<SheetData>().First();

                foreach (var r in sheetData.Elements<Row>())
                {
                    if (r.RowIndex != 1)
                    {
                        var creationDateTime = DateTime.Now;
                        string meterIdentity = null;
                        string reading = null;
                        string customer = null;

                        var column = 1;
                        foreach (var c in r.Elements<Cell>())
                        {
                            switch (column)
                            {
                                case 1:
                                    customer = c.InnerText;
                                    break;
                                case 2:
                                    meterIdentity = c.InnerText;
                                    break;
                                case 3:
                                    creationDateTime = DateTime.ParseExact(c.InnerText, "MM/dd/yyyy hh:mm tt", null).ToUniversalTime();
                                    break;
                                case 4:
                                    reading = c.InnerText;
                                    break;
                                default:
                                    break;
                            }
                            column = column + 1;
                        }

                        try
                        {
                            if (string.IsNullOrEmpty(meterIdentity))
                            {
                                meterIdentity = currentContext.Customers.FirstOrDefault(x => x.CustomerNumber == customer).Meter.MeterIdentity;
                            }

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
                            EventSourceWriter.Log.MessageMethod($"EXCEPTION: WaterMeterExcelProcessor_Ovod. {e.Message}");
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
                                    Reading = reading,
                                    Encrypted = false,
                                    DMA = thisDma.Identifier
                                };

                                const string strWriteConnectionString = "MKWDNConnectionString";
                                var blAttempt = WriteMessageMeterDataToDataTable(sm, strWriteConnectionString,
                                    thisDma.Site.TableName);

                                if (!blAttempt)
                                {
                                    EventSourceWriter.Log.MessageMethod(
                                        $"ERROR writing to BLOB storage by WaterMeterExcelProcessor_Ovod. Received message Id: {receivedMessage.Id}BLOB name: {blob.Name}, PartitionKey: {sm.PartitionKey}");
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            EventSourceWriter.Log.MessageMethod(
                                $"ERROR writing to BLOB storage in WaterMeterExcelProcessor_Ovod. Received message Id: {receivedMessage.Id}BLOB name: {blob.Name}, Exception: {e.Message}");
                        }
                    }
                }
            }

            EventSourceWriter.Log.MessageMethod("WaterMeterExcelProcessor_Ovod Message Processing Complete. MessageId:  " + receivedMessage.Id);

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
