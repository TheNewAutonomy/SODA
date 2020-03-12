using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace RabbitMQConnector
{
    public enum RoutingType
    {
        Get,
        Put,
        List,
        Monitoring,
        Unknown
    };

    public class RequestManager
    {
        public string Message { get; set; }

        public Dictionary<string, string> RootElements;

        public List<Dictionary<string, string>> Records;
        
        public RoutingType Method { get; set; }

        public int Limit { get; set; }

        public RequestManager(string message, string routingKey)
        {
            if (!string.IsNullOrEmpty(message) &&
                !string.IsNullOrEmpty(routingKey) &&
                routingKey.StartsWith("uw.service.cds"))
            {
                if (routingKey.EndsWith("get"))
                {
                    Method = RoutingType.Get;
                }
                else if (routingKey.EndsWith("put") || routingKey.EndsWith("update"))
                {
                    Method = RoutingType.Put;
                }
                else if (routingKey.EndsWith("list"))
                {
                    Method = RoutingType.List;
                }
                else if (String.CompareOrdinal(routingKey, "uw.service.cds.monitoring.xml") == 0)
                {
                    Method = RoutingType.Monitoring;
                }
                else
                {
                    Method = RoutingType.Unknown;
                }

                if (Method != RoutingType.Unknown &&
                    Method != RoutingType.Monitoring)
                {
                    Message = message;

                    RootElements = new Dictionary<string, string>();
                    Records = new List<Dictionary<string, string>>();
                    ParseMessage();
                }
            }
            else
            {
                Method = RoutingType.Unknown;
            }
        }

        public string GetRidOfUnprintablesAndUnicode(string inpString)
        {
            return inpString.Where(ch => ((int) (byte) ch) >= 32 & ((int) (byte) ch) <= 128).Aggregate(string.Empty, (current, ch) => current + ch);
        }

        private void ParseMessage()
        {
            if (Message.ToLower().Contains("<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n<request>"))
            {
                Message = GetRidOfUnprintablesAndUnicode(Message);
            }

            var root = XElement.Parse(Message);

            switch(Method)
            {
                case RoutingType.Get:
                case RoutingType.List:
                    var xElement = root.Element("query");
                    if (xElement != null)
                        foreach (var thisElement in xElement.Elements())
                        {
                            RootElements.Add(thisElement.Name.ToString(), thisElement.Value);
                        }
                    break;

                case RoutingType.Put:
                    var element = root.Element("recordSet");
                    if (element != null)
                        foreach (var thisElement in element.Elements())
                        {
                            switch (thisElement.Name.ToString())
                            {
                                case "metadata":
                                    foreach(var thisAttribute in thisElement.Attributes())
                                    {
                                        RootElements.Add(thisAttribute.Name.ToString(), thisAttribute.Value);
                                    }
                                    break;

                                case "record":
                                    var newRecord = new Dictionary<string, string>();

                                    foreach (var subElement in thisElement.Elements())
                                    {
                                        if (subElement.Name == "variable" && subElement.HasAttributes == true)
                                        {
                                            foreach(var subsubElement in subElement.Elements())
                                            {
                                                newRecord.Add(subElement.FirstAttribute.Value + "_" + subsubElement.Name, subsubElement.Value);
                                            }
                                        }
                                        else
                                        {
                                            if (subElement.HasElements &&
                                                subElement.Elements().Any(x => x.Name == "name") &&
                                                subElement.Elements().Any(x => x.Name == "value"))
                                            {
                                                newRecord.Add(subElement.Elements().FirstOrDefault(x => x.Name == "name")?.Value + "_value",
                                               subElement.Elements().FirstOrDefault(x => x.Name == "value")?.Value);
                                            }
                                            else
                                            {
                                                newRecord.Add(subElement.Name.ToString(), subElement.Value);
                                            }
                                        }
                                    }

                                    Records.Add(newRecord);
                                    break;

                                default:
                                    RootElements.Add(thisElement.Name.ToString(), thisElement.Value);
                                    break;
                            }
                        }

                    break;

                case RoutingType.Unknown:
                    break;
            }
        }
    }
}
