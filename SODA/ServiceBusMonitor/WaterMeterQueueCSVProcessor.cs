using CsvHelper;
using DataAccess;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TableDataAccess;

namespace ServiceBusMonitor
{
    public class WaterMeterQueueCSVProcessor : QueueProcessorBase, IQueueProcessor
    {
        public void ProcessQueue(CloudBlockBlob blob,
                                 string strContainer,
                                 CloudQueueMessage receivedMessage,
                                 CloudQueue urbanWaterQueue)
        {
            EventSourceWriter.Log.MessageMethod("Worker Role Prcessing HeadEndSystem file " + receivedMessage.Id.ToString() + " " + blob.Name);
            
            SQLAzureDataContext currentContext = new SQLAzureDataContext();

            using (TextReader sr = new StringReader(blob.DownloadText()))
            {
                using (var csv = new CsvReader(sr))
                {
                    csv.Configuration.Delimiter = ";";
                    while (csv.Read())
                    {
                        try
                        {
                            MeterReadingEntity sm = new MeterReadingEntity();
                            DMAMeterReadingEntity dmasm = new DMAMeterReadingEntity();
                            if (csv.CurrentRecord[0] != null &&
                                csv.CurrentRecord[3] != null &&
                                csv.CurrentRecord[4] != null &&
                                csv.CurrentRecord[5] != null &&
                                string.Compare(csv.CurrentRecord[5], "LITER") == 0)
                            {
                                DateTime creationDateTime = DateTime.ParseExact(csv.CurrentRecord[3], "dd/MM/yyyy HH:mm:ss", null).ToUniversalTime();

//                                DMAMeterTableStorageContext dmamcontext = new DMAMeterTableStorageContext(strContainer + "_dmameter");
                                // DMAMeterEntity dmaMeterEntity = dmamcontext.MeterReadings.FirstOrDefault(x => x.PartitionKey == csv.CurrentRecord[0]);
                                string dmaId = currentContext.DMAs.Where(x => x.Name == "El Toyo – Retamar").FirstOrDefault().Identifier;
 
                                sm.PartitionKey = csv.CurrentRecord[0];
                                sm.CreatedOn = creationDateTime;
                                sm.RowKey = creationDateTime.Ticks.ToString();
                                sm.Reading = csv.CurrentRecord[4];
                                sm.Encrypted = false;
                                sm.DMA = dmaId;

                                dmasm.PartitionKey = dmaId;
                                dmasm.CreatedOn = creationDateTime;
                                dmasm.RowKey = creationDateTime.Ticks.ToString();
                                dmasm.Reading = csv.CurrentRecord[4];
                                dmasm.Encrypted = false;
                                dmasm.MeterID = csv.CurrentRecord[0];
                            }
                            else
                            {
                                EventSourceWriter.Log.MessageMethod("ERROR: Null values parsed in HeadEndSystem file  " + blob.Name);
                            }

                            string strWriteConnectionString = "MKWDNConnectionString";
                            bool blAttempt = WriteMessageMeterDataToDataTable(sm, dmasm, strWriteConnectionString, strContainer);

                            if (!blAttempt)
                            {
                                EventSourceWriter.Log.MessageMethod("ERROR:Processing Entry NOT added to storage " + receivedMessage.Id.ToString());
                                EventSourceWriter.Log.MessageMethod("ERROR:Processing Entry NOT added to storage " + blob.Name + " " + sm.PartitionKey);
                            }
                        }
                        catch (Exception e)
                        {
                            EventSourceWriter.Log.MessageMethod("Exception processing meter reading: " + e.Message);
                        }
                    }
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
