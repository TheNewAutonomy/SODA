using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ThreadedRole;

namespace RabbitMQConnector
{
    /*
    public class RabbitMQClient : WorkerEntryPoint
    {
        public readonly CancellationTokenSource cancellationToken = new CancellationTokenSource();
        private Monitoring monitor = new Monitoring();
        private IConnection conn;

        public RabbitMQClient()
        {

        }

        public override void Run()
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                while (conn == null && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        ConnectionFactory cf = new ConnectionFactory();
                        cf.UserName = "red";
                        cf.Password = "dgug135asf";
                        cf.VirtualHost = "/UW-DEV";
                        cf.HostName = "dev.urbanwater-ict.eu";
                        cf.Port = 5672;
                        cf.AutomaticRecoveryEnabled = true;
                        cf.RequestedHeartbeat = 30;
                        cf.Uri = "amqp://red:dgug135asf@dev.urbanwater-ict.eu:5672/";
                        conn = cf.CreateConnection();

                        EventSourceWriter.Log.MessageMethod("RabbitMQ connected");
                    }
                    catch (Exception e)
                    {
                        EventSourceWriter.Log.MessageMethod("Failed to connect to RabbitMQ: " + e.Message);
                    }

                    if (conn == null)
                    {
                        Thread.Sleep(600000);
                    }
                }

                if (!cancellationToken.IsCancellationRequested && conn.IsOpen == true)
                {
                    using (conn)
                    {
                        using (IModel ch = conn.CreateModel())
                        {
                            string queueName = ensureQueue(ch);

                            using (Subscription sub = new Subscription(ch, queueName))
                            {
                                string responseMsg = null;

                                // Keep looping until we get a cancel notification
                                while (!cancellationToken.IsCancellationRequested && conn != null && conn.IsOpen == true)
                                {
                                    Trace.TraceInformation("Working");

                                    // Get next message
                                    BasicDeliverEventArgs nextMessage = sub.Next();

                                    RequestManager newRequestManager = null;
                                    try
                                    {
                                        newRequestManager = new RequestManager(messageText(nextMessage).ToLower(), nextMessage.RoutingKey.ToLower());
                                    }
                                    catch (Exception e)
                                    {
                                        responseMsg = getErrorResponse("Error: " + e.Message);
                                        EventSourceWriter.Log.MessageMethod("XML message recieved and failed to parse. Received message: " + messageText(nextMessage) + ", Routing Key: " + nextMessage.RoutingKey.ToLower() + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo + ", Exception: " + e.Message);
                                    }

                                    if (newRequestManager != null &&
                                        newRequestManager.Method != RoutingType.Unknown)
                                    {
                                        IFormatProvider cultureInfo = new CultureInfo("en-GB", false);

                                        try
                                        {
                                            switch (nextMessage.RoutingKey.ToLower())
                                            {
                                                case "uw.service.cds.meterdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    MeterManager newMeterManager = new MeterManager(newRequestManager);
                                                    responseMsg = newMeterManager.Read();
                                                    break;

                                                case "uw.service.cds.dmameterdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    MeterManager newdmaMeterManager = new MeterManager(newRequestManager);
                                                    responseMsg = newdmaMeterManager.DMARead();
                                                    break;

                                                case "uw.service.cds.meter.list":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    MeterManager newMeterManagerList = new MeterManager(newRequestManager);
                                                    responseMsg = newMeterManagerList.ReadList();
                                                    break;

                                                case "uw.service.cds.dmadata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    DMADataManager newDMADataManagerPut = new DMADataManager(newRequestManager);
                                                    newDMADataManagerPut.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    string newnewdmadata = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newdmadata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                                    sendMessages(ch, "uw.event.cds.newdmadata", nextMessage, newnewdmadata);
                                                    break;

                                                case "uw.service.cds.dmadata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    DMADataManager newDMADataManagerGet = new DMADataManager(newRequestManager);
                                                    responseMsg = newDMADataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.dma.list":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    DMAManager newDMAManagerPut = new DMAManager(newRequestManager);
                                                    responseMsg = newDMAManagerPut.ReadList();
                                                    break;

                                                case "uw.service.cds.dma.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    DMAManager newDMAManagerGet = new DMAManager(newRequestManager);
                                                    responseMsg = newDMAManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.reservoirdata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    ReservoirDataManager newReservoirDataManagerPut = new ReservoirDataManager(newRequestManager);
                                                    newReservoirDataManagerPut.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    string newreservoirdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newreservoirdata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                                    sendMessages(ch, "uw.event.cds.newreservoirdata", nextMessage, newreservoirdataMsg);
                                                    break;

                                                case "uw.service.cds.reservoirdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    ReservoirDataManager newReservoirDataManagerGet = new ReservoirDataManager(newRequestManager);
                                                    responseMsg = newReservoirDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.tankdata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    TankDataManager newTankDataManagerPut = new TankDataManager(newRequestManager);
                                                    newTankDataManagerPut.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    string tankdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newtankdata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                                    sendMessages(ch, "uw.event.cds.newtankdata", nextMessage, tankdataMsg);
                                                    break;

                                                case "uw.service.cds.tankdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    TankDataManager newTankDataManagerGet = new TankDataManager(newRequestManager);
                                                    responseMsg = newTankDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.weatherdata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    WeatherDataManager newWeatherDataManagerPut = new WeatherDataManager(newRequestManager);
                                                    newWeatherDataManagerPut.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    string weatherdatatankdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newweatherdata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                                    sendMessages(ch, "uw.event.cds.newweatherdata", nextMessage, weatherdatatankdataMsg);
                                                    break;

                                                case "uw.service.cds.weatherdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    WeatherDataManager newWeatherDataManagerGet = new WeatherDataManager(newRequestManager);
                                                    responseMsg = newWeatherDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.weatherforecastdata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    WeatherForecastDataManager newWeatherForecastDataManagerManagerPut = new WeatherForecastDataManager(newRequestManager);
                                                    newWeatherForecastDataManagerManagerPut.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    string newweatherforecastdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newweatherforecastdata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                                    sendMessages(ch, "uw.event.cds.newweatherforecastdata", nextMessage, newweatherforecastdataMsg);
                                                    break;

                                                case "uw.service.cds.weatherforecastdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    WeatherForecastDataManager newWeatherForecastDataManagerManagerGet = new WeatherForecastDataManager(newRequestManager);
                                                    responseMsg = newWeatherForecastDataManagerManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.demandpredictiondata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    DemandPredictionDataManager newDemandPredictionDataManagerSet = new DemandPredictionDataManager(newRequestManager);
                                                    newDemandPredictionDataManagerSet.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    string newdemandpredictiondataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newdemandpredictiondata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                                    sendMessages(ch, "uw.event.cds.newdemandpredictiondata", nextMessage, newdemandpredictiondataMsg);
                                                    break;

                                                case "uw.service.cds.demandpredictiondata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    DemandPredictionDataManager newDemandPredictionDataManagerGet = new DemandPredictionDataManager(newRequestManager);
                                                    responseMsg = newDemandPredictionDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.nearestneighbours.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    DemandPredictionDataManager newDemandPredictionDataManagerNN = new DemandPredictionDataManager(newRequestManager);
                                                    responseMsg = newDemandPredictionDataManagerNN.GetNearestNeighboursResponse();
                                                    break;

                                                case "uw.service.cds.consumptioncharacterizationdata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    ConsumptionCharacterizationDataManager newConsumptionCharacterizationDataManagerSet = new ConsumptionCharacterizationDataManager(newRequestManager);
                                                    newConsumptionCharacterizationDataManagerSet.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    string newconsumptioncharacterizationdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newconsumptioncharacterizationdata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                                    sendMessages(ch, "uw.event.cds.newconsumptioncharacterizationdata", nextMessage, newconsumptioncharacterizationdataMsg);
                                                    break;

                                                case "uw.service.cds.consumptioncharacterizationdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    ConsumptionCharacterizationDataManager newConsumptionCharacterizationDataManagerGet = new ConsumptionCharacterizationDataManager(newRequestManager);
                                                    responseMsg = newConsumptionCharacterizationDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.availabilitypredictiondata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    AvailabilityPredictionDataManager newAvailabilityPredictionDataManagerPut = new AvailabilityPredictionDataManager(newRequestManager);
                                                    newAvailabilityPredictionDataManagerPut.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    string newavailabilitypredictiondataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newavailabilitypredictiondata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                                    sendMessages(ch, "uw.event.cds.newavailabilitypredictiondata", nextMessage, newavailabilitypredictiondataMsg);
                                                    break;

                                                case "uw.service.cds.availabilitypredictiondata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    AvailabilityPredictionDataManager newAvailabilityPredictionDataManagerGet = new AvailabilityPredictionDataManager(newRequestManager);
                                                    responseMsg = newAvailabilityPredictionDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.dmaleakagedata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    DMALeakageDataManager newDMALeakageDataManagerSet = new DMALeakageDataManager(newRequestManager);
                                                    newDMALeakageDataManagerSet.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    string newdmaleakagedataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newdmaleakagedata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                                    sendMessages(ch, "uw.event.cds.newdmaleakagedata", nextMessage, newdmaleakagedataMsg);
                                                    break;

                                                case "uw.service.cds.dmaleakagedata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    DMALeakageDataManager newDMALeakageDataManagerGet = new DMALeakageDataManager(newRequestManager);
                                                    responseMsg = newDMALeakageDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.pricingdata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    PricingDataManager newPricingDataManagerSet = new PricingDataManager(newRequestManager);
                                                    newPricingDataManagerSet.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    string newpricingdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newpricingdata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                                    sendMessages(ch, "uw.event.cds.newpricingdata", nextMessage, newpricingdataMsg);
                                                    break;

                                                case "uw.service.cds.pricingdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + messageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();
                                                    
                                                    PricingDataManager newPricingDataManager = new PricingDataManager(newRequestManager);
                                                    responseMsg = newPricingDataManager.Read();
                                                    break;

                                                case "uw.service.cds.monitoring.xml":
                                                    // Process message
                                                    string monitoringResponseMsg = monitor.GetServiceStartDateTime();

                                                    //Send response
                                                    sendMonitoringMessages(ch, queueName, nextMessage, monitoringResponseMsg);
                                                    sub.Ack();
                                                    
                                                    break;

                                                default:
                                                    break;
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            EventSourceWriter.Log.MessageMethod(nextMessage.RoutingKey + " exception: " + e.Message);
                                            responseMsg = "<response>" +
                                                            "<status>Error</status>" +
                                                          "</response>";
                                        }
                                    }

                                    if (!string.IsNullOrEmpty(responseMsg))
                                    {
                                        //Send response
                                        sendMessages(ch, nextMessage.BasicProperties.ReplyTo, nextMessage, responseMsg);
                                    }

                                    responseMsg = null;

                                    if (monitor.CheckMeteringDispatch())
                                    {
                                        string newMessage = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                "<cloud_data_service>" +
                                                                    "<status>" +
                                                                        "uw.event.cds.newmeterdata" +
                                                                    "</status>" +
                                                                "</cloud_data_service>";

                                        sendMessages(ch, "uw.event.cds.newmeterdata", nextMessage, newMessage);
                                    }
                                }
                            }
                        }
                    }
                    Trace.TraceInformation("Working");
                }
            }
        }

        private static void sendMessagesTest(IModel ch, string queueName, string message)
        {
            IBasicProperties props = ch.CreateBasicProperties();
            props.CorrelationId = "1234";
            props.Priority = 0;
            props.ReplyTo = "uw.service.cds.meterdata";
            props.ReplyTo = "uw.service.cds.demandpredictiondata";
            props.DeliveryMode = 2;
            props.Timestamp = DateExtensions.ToAmqpTimestamp(DateTime.Now);

            Dictionary<string, object> bindingOneHeaders = new Dictionary<string, object>();
            bindingOneHeaders.Add("UW-MSG-TYPE", 2);
            props.Headers = bindingOneHeaders;
            props.ContentType = "application/xml";

            ch.BasicPublish("UW-DEFAULT-EXCHANGE", queueName, props, Encoding.UTF8.GetBytes(message));
        }

        private static void sendMonitoringMessages(IModel ch, string queueName, BasicDeliverEventArgs nextMessage, string responseMsg)
        {
            IBasicProperties props = ch.CreateBasicProperties();
            props.CorrelationId = nextMessage.BasicProperties.CorrelationId;
            props.ReplyTo = "uw.service.cds.monitoring.xml";
            props.Priority = 0;
            props.DeliveryMode = 2;
            props.Timestamp = DateExtensions.ToAmqpTimestamp(DateTime.Now);

            Dictionary<string, object> bindingOneHeaders = new Dictionary<string, object>();
            bindingOneHeaders.Add("UW-MSG-TYPE", 2);
            props.Headers = bindingOneHeaders;
            props.ContentType = "application/xml";

            ch.BasicPublish("UW-DEFAULT-EXCHANGE", nextMessage.BasicProperties.ReplyTo, props, Encoding.UTF8.GetBytes(responseMsg));
        }

        private static void sendMessages(IModel ch, string queueName, BasicDeliverEventArgs nextMessage, string responseMsg)
        {
            IBasicProperties props = ch.CreateBasicProperties();
            props.CorrelationId = nextMessage.BasicProperties.CorrelationId;
            props.ReplyTo = "uw.service.cds";

            props.Priority = 0;
            props.DeliveryMode = 2;
            props.Timestamp = DateExtensions.ToAmqpTimestamp(DateTime.Now);

            Dictionary<string, object> bindingOneHeaders = new Dictionary<string, object>();
            bindingOneHeaders.Add("UW-MSG-TYPE", 2);
            props.Headers = bindingOneHeaders;
            props.ContentType = "application/xml";

            ch.BasicPublish("UW-DEFAULT-EXCHANGE", queueName, props, Encoding.UTF8.GetBytes(responseMsg));

            EventSourceWriter.Log.MessageMethod("RoutingKey: " + queueName + ", Message: " + responseMsg);
        }

        private static string messageText(BasicDeliverEventArgs ev)
        {
            if (ev != null && ev.Body != null)
            {
                return Encoding.UTF8.GetString(ev.Body);
            }
            else
            {
                return null;
            }
        }

        private static string ensureQueue(IModel ch)
        {
            string queueName = "CloudDataService";
            ch.QueueBind(queueName, "UW-DEFAULT-EXCHANGE", queueName);
            return queueName;
        }

        private static string getErrorResponse(string message)
        {
            return "<response>" +
                        "<Error>" + message + "</Error>" +
                   "</response>";
        }
    }


    /// <summary>
    /// Date Extension Methods
    /// </summary>
    public static class DateExtensions
    {
        /// <summary>Helper method to convert from DateTime to AmqpTimestamp.</summary>
        /// <param name="datetime">The datetime.</param>
        /// <returns>The AmqpTimestamp.</returns>
        internal static AmqpTimestamp ToAmqpTimestamp(this DateTime datetime)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var unixTime = (datetime.ToUniversalTime() - epoch).TotalSeconds;
            var timestamp = new AmqpTimestamp((long)unixTime);
            return timestamp;
        }

        /// <summary>Helper method to convert from AmqpTimestamp.UnixTime to a DateTime (for the local machine).</summary>
        /// <param name="timestamp">The timestamp.</param>
        /// <returns>The DateTime.</returns>
        internal static DateTime ToDateTime(this AmqpTimestamp timestamp)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return epoch.AddSeconds(timestamp.UnixTime).ToLocalTime();
        }

        /// <summary>The to milliseconds.</summary>
        /// <param name="datetime">The datetime.</param>
        /// <returns>The System.Int64.</returns>
        public static long ToMilliseconds(this DateTime datetime)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var unixTime = (datetime.ToUniversalTime() - epoch).TotalMilliseconds;
            return (long)unixTime;
        }
    }
     */
}
