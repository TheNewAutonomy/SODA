
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.ServiceRuntime;
using System.Diagnostics.Tracing;

namespace RabbitMQConnector
{
    [EventSource(Name = "RabbitMQWorkerRoleSource")]
    sealed class EventSourceWriter : EventSource
    {
        public static EventSourceWriter Log = new EventSourceWriter();
        public void MessageMethod(string message)
        {
            if (IsEnabled())
                WriteEvent(1, message);
        }
    }

    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent _runCompleteEvent = new ManualResetEvent(false);

        private IConnection _conn;
        private readonly Monitoring _monitor = new Monitoring();

        public override void Run()
        {
            Trace.TraceInformation("RabbitMQWorkerRole is running");

            try
            {
                RunAsync(_cancellationTokenSource.Token).Wait();
            }
            catch (Exception e)
            {
                EventSourceWriter.Log.MessageMethod("Run Exception: " + e.Message);
            }
            finally
            {
                _runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            var result = false;

            try
            {
                // Set the maximum number of concurrent connections
                ServicePointManager.DefaultConnectionLimit = 12;

                // For information on handling configuration changes
                // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

                result = base.OnStart();

                Trace.TraceInformation("RabbitMQWorkerRole has been started");

                _monitor.SetStartDateTime();
            }
            catch (Exception e)
            {
                EventSourceWriter.Log.MessageMethod("OnStart Exception: " + e.Message);
            }
            return result;
        }

        public override void OnStop()
        {
            try
            {
                Trace.TraceInformation("RabbitMQWorkerRole is stopping");

                _cancellationTokenSource.Cancel();
                _runCompleteEvent.WaitOne();

                base.OnStop();

                _conn?.Close();
                Trace.TraceInformation("RabbitMQWorkerRole has stopped");
            }
            catch (Exception e)
            {
                EventSourceWriter.Log.MessageMethod("OnStop Exception: " + e.Message);
            }
        }
        
        private async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                /*
//                long counterxxx = MeterManager.CountEntries();
                
                RequestManager newRequestManagerX = null;
                try
                {
                    var teststringx = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                      "<request>" +
                                          "<method>UPDATE</method>" +
                                          "<recordSet>" +
                                             "<elementId>072207070013</elementId>" +
                                             "<record>" +
                                                  "<start>2016-02-21 17:32:05.0000000+01:00</start>" +
                                                  "<variable name=\"characterized_device\">" +
                                                      "<value>Washing Machine</value>" +
                                                  "</variable>" +
                                                  "<variable name=\"confirmed_device\">" +
                                                      "<value>Washing Machine</value>" +
                                                  "</variable>" +
                                                  "<variable name=\"confirmation\">" +
                                                      "<value>yes</value>" +
                                                  "</variable>" +
                                             "</record>" +
                                          "</recordSet>" +
                                       "</request>";
                    
                    newRequestManagerX = new RequestManager(teststringx, "uw.service.cds.consumptioncharacterizationdata.update");
                  

                    var newConsumptionCharacterizationDataManagerSetUpdateX = new ConsumptionCharacterizationDataManager(newRequestManagerX);
                    newConsumptionCharacterizationDataManagerSetUpdateX.Update();

                }
                catch (Exception e)
                {
                    
                }
                */
                while (_conn == null && !cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var cf = new ConnectionFactory
                        {
                            UserName = "red",
                            Password = "dgug135asf",
                            VirtualHost = "/UW-DEV",
                            HostName = "dev.urbanwater-ict.eu",
                            Port = 5672,
                            AutomaticRecoveryEnabled = true,
                            RequestedHeartbeat = 30,
                            Uri = "amqp://red:dgug135asf@dev.urbanwater-ict.eu:5672/"
                        };
                        _conn = cf.CreateConnection();

                        EventSourceWriter.Log.MessageMethod("RabbitMQ connected");
                    }
                    catch (Exception e)
                    {
                        EventSourceWriter.Log.MessageMethod("Failed to connect to RabbitMQ: " + e.Message);
                    }

                    if (_conn == null)
                    {
                        var newspan = new TimeSpan(0, 10, 0);
                        await Task.Delay(newspan, cancellationToken);
                    }
                }

                if (!cancellationToken.IsCancellationRequested && _conn.IsOpen)
                {
                    using (_conn)
                    {
                        using (var ch = _conn.CreateModel())
                        {
                            var queueName = EnsureQueue(ch);

                            using (var sub = new Subscription(ch, queueName))
                            {
                                string responseMsg = null;

                                // Keep looping until we get a cancel notification
                                while (!cancellationToken.IsCancellationRequested && _conn != null && _conn.IsOpen)
                                {
                                    Trace.TraceInformation("Working");

                                    // Get next message
                                    var nextMessage = sub.Next();

                                    RequestManager newRequestManager = null;
                                    try
                                    {
                                        newRequestManager = new RequestManager(MessageText(nextMessage), nextMessage.RoutingKey.ToLower());
                                    }
                                    catch (Exception e)
                                    {
                                        responseMsg = GetErrorResponse("Error: " + e.Message);
                                        EventSourceWriter.Log.MessageMethod(
                                            $"XML message recieved and failed to parse. Received message: {MessageText(nextMessage)}, Routing Key: {nextMessage.RoutingKey.ToLower()}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}, Exception: {e.Message}");
                                    }

                                    if (newRequestManager != null &&
                                        newRequestManager.Method != RoutingType.Unknown)
                                    {
                                        try
                                        {
                                            switch (nextMessage.RoutingKey.ToLower())
                                            {
                                                case "uw.service.cds.dmarelateddata.get":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();
                                                    // meter Id in
                                                    // get Dma Id
                                                    // Get resource by that Id
                                                    var newDataManager = new DMARelatedDataManager(newRequestManager);

                                                    var responseString = string.Empty;

                                                    if (newDataManager.Indices.Contains(1))
                                                    {
                                                        var newMeterManagerDma = new MeterManager(newRequestManager);
                                                        responseString = newMeterManagerDma.DMARead(false);
                                                    }

                                                    if (newDataManager.Indices.Contains(2) ||
                                                        newDataManager.Indices.Contains(3) ||
                                                        newDataManager.Indices.Contains(4) ||
                                                        newDataManager.Indices.Contains(5) ||
                                                        newDataManager.Indices.Contains(6) ||
                                                        newDataManager.Indices.Contains(7) ||
                                                        newDataManager.Indices.Contains(8))
                                                    {
                                                        var newWeatherDataManagerGetDma = new WeatherDataManager(newRequestManager);
                                                        responseString += newWeatherDataManagerGetDma.ReadSpecificProperty(newDataManager.Indices);
                                                    }
                                                    
                                                    if (newDataManager.Indices.Contains(9) ||
                                                        newDataManager.Indices.Contains(10) ||
                                                        newDataManager.Indices.Contains(11) ||
                                                        newDataManager.Indices.Contains(12) ||
                                                        newDataManager.Indices.Contains(13) ||
                                                        newDataManager.Indices.Contains(14) ||
                                                        newDataManager.Indices.Contains(15))
                                                    {
                                                        var newWeatherDataManagerGetDma = new WeatherForecastDataManager(newRequestManager);
                                                        responseString += newWeatherDataManagerGetDma.ReadSpecificProperty(newDataManager.Indices);
                                                    }

                                                    if (newDataManager.Indices.Contains(16) ||
                                                        newDataManager.Indices.Contains(17) ||
                                                        newDataManager.Indices.Contains(18))
                                                    {
                                                        var newDmaManagerGetDma = new DMADataManager(newRequestManager);
                                                        responseString += newDmaManagerGetDma.ReadSpecificProperty(newDataManager.Indices);
                                                    }

                                                    if (newDataManager.Indices.Contains(19))
                                                    {
                                                        var newDemandPredictionManagerGetDma = new DemandPredictionDataManager(newRequestManager);
                                                        responseString += newDemandPredictionManagerGetDma.ReadSpecificProperty(newDataManager.Indices);
                                                    }

                                                    if (newDataManager.Indices.Contains(20))
                                                    {
                                                        var newLeakageDataManagerGetDma = new DMALeakageDataManager(newRequestManager);
                                                        responseString += newLeakageDataManagerGetDma.ReadSpecificProperty(newDataManager.Indices);
                                                    }

                                                    responseMsg = "<response>" +
                                                                  $"<elementId>{newDataManager.ElementId}</elementId>" +
                                                                  $"<recordSet>{responseString}</recordSet>" +
                                                                  "</response>";
                                                    break;

                                                case "uw.service.cds.meterdata.get":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newMeterManager = new MeterManager(newRequestManager);
                                                    responseMsg = newMeterManager.Read(true);
                                                    break;
                                                    /*
                                                case "uw.service.cds.meterdataincrements.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    MeterManager newMeterManagerincrements = new MeterManager(newRequestManager);
                                                    responseMsg = newMeterManagerincrements.ReadIncrements(true);
                                                    break;
                                                    */
                                                case "uw.service.cds.dmameterdata.get":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newdmaMeterManager = new MeterManager(newRequestManager);
                                                    responseMsg = newdmaMeterManager.DMARead(true);
                                                    break;

                                                case "uw.service.cds.meter.list":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newMeterManagerList = new MeterManager(newRequestManager);
                                                    responseMsg = newMeterManagerList.ReadList();
                                                    break;

                                                case "uw.service.cds.dmadata.put":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newDmaDataManagerPut = new DMADataManager(newRequestManager);
                                                    newDmaDataManagerPut.Create();

                                                    responseMsg = "<response><status>Success</status></response>";

                                                    const string newnewdmadata = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                                 "<cloud_data_service><status>uw.event.cds.newdmadata</status></cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newdmadata", nextMessage, newnewdmadata);
                                                    break;

                                                case "uw.service.cds.dmadata.get":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newDmaDataManagerGet = new DMADataManager(newRequestManager);
                                                    responseMsg = newDmaDataManagerGet.Read();
                                                    break;
                                                    
                                                case "uw.service.cds.dma.list":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newDmaManagerPut = new DMAManager(newRequestManager);
                                                    responseMsg = newDmaManagerPut.ReadList();
                                                    break;

                                                case "uw.service.cds.dma.get":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newDmaManagerGet = new DMAManager(newRequestManager);
                                                    responseMsg = newDmaManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.reservoirdata.put":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newReservoirDataManagerPut = new ReservoirDataManager(newRequestManager);
                                                    newReservoirDataManagerPut.Create();

                                                    responseMsg ="<response><status>Success</status></response>";

                                                    const string newreservoirdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                                       "<cloud_data_service><status>uw.event.cds.newreservoirdata</status></cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newreservoirdata", nextMessage, newreservoirdataMsg);
                                                    break;

                                                case "uw.service.cds.reservoirdata.get":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newReservoirDataManagerGet = new ReservoirDataManager(newRequestManager);
                                                    responseMsg = newReservoirDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.tankdata.put":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newTankDataManagerPut = new TankDataManager(newRequestManager);
                                                    newTankDataManagerPut.Create();

                                                    responseMsg = "<response><status>Success</status></response>";

                                                    const string tankdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                               "<cloud_data_service><status>uw.event.cds.newtankdata</status></cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newtankdata", nextMessage, tankdataMsg);
                                                    break;

                                                case "uw.service.cds.tankdata.get":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newTankDataManagerGet = new TankDataManager(newRequestManager);
                                                    responseMsg = newTankDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.weatherdata.put":
                                                    EventSourceWriter.Log.MessageMethod(
                                                        $"RoutingKey: {nextMessage.RoutingKey} , Inbound: {MessageText(nextMessage)}, ReplyTo: {nextMessage.BasicProperties.ReplyTo}");
                                                    sub.Ack();

                                                    var newWeatherDataManagerPut = new WeatherDataManager(newRequestManager);
                                                    newWeatherDataManagerPut.Create();

                                                    responseMsg = "<response><status>Success</status></response>";

                                                    const string weatherdatatankdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                                          "<cloud_data_service>" +
                                                                                          "<status>" +
                                                                                          "uw.event.cds.newweatherdata" +
                                                                                          "</status>" +
                                                                                          "</cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newweatherdata", nextMessage, weatherdatatankdataMsg);
                                                    break;

                                                case "uw.service.cds.weatherdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newWeatherDataManagerGet = new WeatherDataManager(newRequestManager);
                                                    responseMsg = newWeatherDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.weatherforecastdata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newWeatherForecastDataManagerManagerPut = new WeatherForecastDataManager(newRequestManager);
                                                    newWeatherForecastDataManagerManagerPut.Create();

                                                    responseMsg = "<response><status>Success</status></response>";

                                                    const string newweatherforecastdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                                             "<cloud_data_service><status>uw.event.cds.newweatherforecastdata</status></cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newweatherforecastdata", nextMessage, newweatherforecastdataMsg);
                                                    break;

                                                case "uw.service.cds.weatherforecastdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newWeatherForecastDataManagerManagerGet = new WeatherForecastDataManager(newRequestManager);
                                                    responseMsg = newWeatherForecastDataManagerManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.demandpredictiondata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newDemandPredictionDataManagerSet = new DemandPredictionDataManager(newRequestManager);
                                                    newDemandPredictionDataManagerSet.Create();

                                                    responseMsg = "<response><status>Success</status></response>";

                                                    const string newdemandpredictiondataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                                              "<cloud_data_service><status>uw.event.cds.newdemandpredictiondata</status></cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newdemandpredictiondata", nextMessage, newdemandpredictiondataMsg);
                                                    break;

                                                case "uw.service.cds.demandpredictiondata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newDemandPredictionDataManagerGet = new DemandPredictionDataManager(newRequestManager);
                                                    responseMsg = newDemandPredictionDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.nearestneighbours.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newDemandPredictionDataManagerNn = new DemandPredictionDataManager(newRequestManager);
                                                    responseMsg = newDemandPredictionDataManagerNn.GetNearestNeighboursResponse();
                                                    break;

                                                case "uw.service.cds.consumptioncharacterizationdata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newConsumptionCharacterizationDataManagerSet = new ConsumptionCharacterizationDataManager(newRequestManager);
                                                    newConsumptionCharacterizationDataManagerSet.Create();

                                                    responseMsg = "<response><status>Success</status></response>";

                                                    const string newconsumptioncharacterizationdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                                                         "<cloud_data_service><status>uw.event.cds.newconsumptioncharacterizationdata</status></cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newconsumptioncharacterizationdata", nextMessage, newconsumptioncharacterizationdataMsg);
                                                    break;

                                                case "uw.service.cds.consumptioncharacterizationdata.update":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newConsumptionCharacterizationDataManagerSetUpdate = new ConsumptionCharacterizationDataManager(newRequestManager);
                                                    newConsumptionCharacterizationDataManagerSetUpdate.Update();

                                                    responseMsg = "<response><status>Success</status></response>";

                                                    const string newconsumptioncharacterizationdataUpdateMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                                                         "<cloud_data_service><status>uw.service.cds.consumptioncharacterizationdata.update</status></cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newconsumptioncharacterizationdata", nextMessage, newconsumptioncharacterizationdataUpdateMsg);
                                                    break;

                                                case "uw.service.cds.consumptioncharacterizationdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newConsumptionCharacterizationDataManagerGet = new ConsumptionCharacterizationDataManager(newRequestManager);
                                                    responseMsg = newConsumptionCharacterizationDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.dicmsite.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newdicmsiteget = new ConsumptionCharacterizationDataManager(newRequestManager);
                                                    responseMsg = newdicmsiteget.Readdicmsite();
                                                    break;

                                                case "uw.service.cds.availabilitypredictiondata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newAvailabilityPredictionDataManagerPut = new AvailabilityPredictionDataManager(newRequestManager);
                                                    newAvailabilityPredictionDataManagerPut.Create();

                                                    responseMsg = "<response>" +
                                                                    "<status>Success</status>" +
                                                                  "</response>";

                                                    const string newavailabilitypredictiondataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                                                    "<cloud_data_service><status>uw.event.cds.newavailabilitypredictiondata</status></cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newavailabilitypredictiondata", nextMessage, newavailabilitypredictiondataMsg);
                                                    break;

                                                case "uw.service.cds.availabilitypredictiondata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newAvailabilityPredictionDataManagerGet = new AvailabilityPredictionDataManager(newRequestManager);
                                                    responseMsg = newAvailabilityPredictionDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.dmaleakagedata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newDmaLeakageDataManagerSet = new DMALeakageDataManager(newRequestManager);
                                                    newDmaLeakageDataManagerSet.Create();

                                                    responseMsg = "<response><status>Success</status></response>";

                                                    const string newdmaleakagedataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                                        "<cloud_data_service><status>uw.event.cds.newdmaleakagedata</status></cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newdmaleakagedata", nextMessage, newdmaleakagedataMsg);
                                                    break;

                                                case "uw.service.cds.dmaleakagedata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newDmaLeakageDataManagerGet = new DMALeakageDataManager(newRequestManager);
                                                    responseMsg = newDmaLeakageDataManagerGet.Read();
                                                    break;

                                                case "uw.service.cds.pricingdata.put":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newPricingDataManagerSet = new PricingDataManager(newRequestManager);
                                                    string elements = newPricingDataManagerSet.Create();

                                                    responseMsg = "<response><status>Success</status></response>";

                                                    string newpricingdataMsg = "<?xml version=1.0 encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                                                                     "<cloud_data_service><status>uw.event.cds.newpricingdata</status>" +
                                                                                     "<elements>" + elements + "</elements>" +
                                                                                     "</cloud_data_service>";

                                                    SendMessages(ch, "uw.event.cds.newpricingdata", nextMessage, newpricingdataMsg);
                                                    break;

                                                case "uw.service.cds.pricingdata.get":
                                                    EventSourceWriter.Log.MessageMethod("RoutingKey: " + nextMessage.RoutingKey + " , Inbound: " + MessageText(nextMessage) + ", ReplyTo: " + nextMessage.BasicProperties.ReplyTo);
                                                    sub.Ack();

                                                    var newPricingDataManager = new PricingDataManager(newRequestManager);
                                                    responseMsg = newPricingDataManager.Read();
                                                    break;

                                                case "uw.service.cds.monitoring.xml":
                                                    // Process message
                                                    var monitoringResponseMsg = _monitor.GetServiceStartDateTime();

                                                    //Send response
                                                    SendMonitoringMessages(ch, queueName, nextMessage, monitoringResponseMsg);
                                                    sub.Ack();

                                                    break;

                                                default:
                                                    break;
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            EventSourceWriter.Log.MessageMethod(nextMessage.RoutingKey + " exception: " + e.Message);
                                            responseMsg = "<response><status>Error</status></response>";
                                        }
                                    }

                                    if (!string.IsNullOrEmpty(responseMsg))
                                    {
                                        SendMessages(ch, nextMessage.BasicProperties.ReplyTo, nextMessage, responseMsg);
                                    }

                                    responseMsg = null;

                                    var processedMeterIds = _monitor.CheckMeteringDispatch();
                                    if (!string.IsNullOrEmpty(processedMeterIds))
                                    {
                                        var newMessage =
                                            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                                            "<cloud_data_service>" + "<status>" + "uw.event.cds.newmeterdata" +
                                            "</status>" + $"<meterId>{processedMeterIds}</meterId>" +
                                            "</cloud_data_service>";

                                        SendMessages(ch, "uw.event.cds.newmeterdata", nextMessage, newMessage);
                                    }

                                    var missingMeterIds = _monitor.CheckMissingMeterIdsDispatch();
                                    if (!string.IsNullOrEmpty(missingMeterIds))
                                    {
                                        var newMessage = "<notificationMessage>" + "<channel>EMAIL</channel>" +
                                                         "<notification>WARNING</notification>" +
                                                         "<title>Readings for unknown meters received</title>" +
                                                         $"<bodyText>{missingMeterIds}</bodyText>" +
                                                         "<username>red</username>" + "</notificationMessage>";

                                        SendMessages(ch, "uw.service.notification", nextMessage, newMessage);
                                    }
                                }
                            }
                        }
                    }
                    Trace.TraceInformation("Working");
                }
            }
        }

        private static void SendMonitoringMessages(IModel ch, string queueName, BasicDeliverEventArgs nextMessage, string responseMsg)
        {
            var props = ch.CreateBasicProperties();
            props.CorrelationId = nextMessage.BasicProperties.CorrelationId;
            props.ReplyTo = "uw.service.cds.monitoring.xml";
            props.Priority = 0;
            props.DeliveryMode = 2;
            props.Timestamp = DateTime.Now.ToAmqpTimestamp();

            var bindingOneHeaders = new Dictionary<string, object> {{"UW-MSG-TYPE", 2}};
            props.Headers = bindingOneHeaders;
            props.ContentType = "application/xml";

            ch.BasicPublish("UW-DEFAULT-EXCHANGE", nextMessage.BasicProperties.ReplyTo, props, Encoding.UTF8.GetBytes(responseMsg));
        }

        private static void SendMessages(IModel ch, string queueName, BasicDeliverEventArgs nextMessage, string responseMsg)
        {
            var props = ch.CreateBasicProperties();
            props.CorrelationId = nextMessage.BasicProperties.CorrelationId;
            props.ReplyTo = "uw.service.cds";

            props.Priority = 0;
            props.DeliveryMode = 2;
            props.Timestamp = DateTime.Now.ToAmqpTimestamp();

            var bindingOneHeaders = new Dictionary<string, object> {{"UW-MSG-TYPE", 2}};
            props.Headers = bindingOneHeaders;
            props.ContentType = "application/xml";

            ch.BasicPublish("UW-DEFAULT-EXCHANGE", queueName, props, Encoding.UTF8.GetBytes(responseMsg));

            EventSourceWriter.Log.MessageMethod($"RoutingKey: {queueName}, message: {responseMsg}");
        }

        private static string MessageText(BasicDeliverEventArgs ev)
        {
            return ev?.Body != null ? Encoding.UTF8.GetString(ev.Body) : null;
        }

        private static string EnsureQueue(IModel ch)
        {
            const string queueName = "CDS";
            return queueName;
        }

        private static string GetErrorResponse(string message)
        {
            return "<response>" + $"<Error>{message}</Error>" + "</response>";
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
}
