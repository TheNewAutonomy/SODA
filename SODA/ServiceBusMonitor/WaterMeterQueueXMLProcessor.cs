using DataAccess;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using TableDataAccess;

namespace ServiceBusMonitor
{
    public class WaterMeterQueueXMLProcessor : QueueProcessorBase, IQueueProcessor
    {
        public void ProcessQueue(CloudBlockBlob    blob,
                                 string            strContainer,
                                 CloudQueueMessage receivedMessage,
                                 CloudQueue        urbanWaterQueue)
        {
            EventSourceWriter.Log.MessageMethod("Worker Role Prcessing Water Meter Reading file " + receivedMessage.Id.ToString() + " " + blob.Name);
            
            SQLAzureDataContext currentContext = new SQLAzureDataContext();

            var xml = blob.DownloadText();
            var xDoc = XDocument.Parse(xml);

            IEnumerable<XElement> root = xDoc.Elements();
            XElement gateways = root.Elements().Where(x => x.Name.LocalName == "gateways").FirstOrDefault();

            try
            {
            foreach (XElement gateway in gateways.Elements().Where(x => x.Name.LocalName == "gateway"))
            {
                XElement collections = gateway.Elements().Where(x => x.Name.LocalName == "collections").FirstOrDefault();
                foreach (XElement collection in collections.Elements().Where(x => x.Name.LocalName == "collection"))
                {
                    string ProducerId = collection.Elements().Where(x => x.Name.LocalName == "producer-id").FirstOrDefault().Value;
                    XElement data = collection.Elements().Where(x => x.Name.LocalName == "data").FirstOrDefault();

                    foreach (XElement record in data.Elements())
                    {
                        string recordDateTime = record.Attributes().Where(x => x.Name.LocalName == "ts").FirstOrDefault().Value;
                        string recordValue = record.Elements().Where(x => x.Name.LocalName == "CURRENT_INDEX").FirstOrDefault().Value;

                        MeterReadingEntity sm = new MeterReadingEntity();
                        DMAMeterReadingEntity dmasm = new DMAMeterReadingEntity();

                        if (!string.IsNullOrEmpty(ProducerId) &&
                            !string.IsNullOrEmpty(recordDateTime) &&
                            !string.IsNullOrEmpty(recordValue)
                            )
                        {
                            string dmaId = currentContext.DMAs.Where(x => x.Name == "Lagar").FirstOrDefault().Identifier;
//                            currentContext.Meters.Where(x => x.MeterIdentity == ProducerId).FirstOrDefault().Utility);
//                            DMAMeterTableStorageContext dmamcontext = new DMAMeterTableStorageContext(container.Name + "_dmameter");
//                            DMAMeterEntity dmaMeterEntity = dmamcontext.MeterReadings.FirstOrDefault(x => x.PartitionKey == ProducerId);

                            sm.PartitionKey = ProducerId;
                            sm.CreatedOn = DateTime.Parse(recordDateTime);
                            sm.RowKey = sm.CreatedOn.Ticks.ToString();
                            sm.Reading = recordValue;
                            sm.Encrypted = false;
                            sm.DMA = dmaId;

                            dmasm.PartitionKey = dmaId;
                            dmasm.CreatedOn = DateTime.Parse(recordDateTime);
                            dmasm.RowKey = dmasm.CreatedOn.Ticks.ToString();
                            dmasm.Reading = recordValue;
                            dmasm.Encrypted = false;
                            dmasm.MeterID = ProducerId;
                        }
                        else
                        {
                            EventSourceWriter.Log.MessageMethod("ERROR: Null values parsed in water meter readings file  " + blob.Name);
                        }

                        string strWriteConnectionString = "MKWDNConnectionString";
                        bool blAttempt = WriteMessageMeterDataToDataTable(sm, dmasm, strWriteConnectionString, strContainer);

                        if (!blAttempt)
                        {
                            EventSourceWriter.Log.MessageMethod("ERROR:Processing Entry NOT added to storage " + receivedMessage.Id.ToString());
                            EventSourceWriter.Log.MessageMethod("ERROR:Processing Entry NOT added to storage " + blob.Name + " " + sm.PartitionKey);
                        }
                    }
                }
            }
            }
            catch(Exception e)
            {
                EventSourceWriter.Log.MessageMethod("ERROR:Processing water meter file:  " + e.Message);
                            
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
