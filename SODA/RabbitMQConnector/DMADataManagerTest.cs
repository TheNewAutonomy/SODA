namespace RabbitMQConnector
{
    public class DMADataManagerTest
    {
        public static string Read()
        {
            return
                "<request>" +
       "<query>" +
       "<dmaId>CC</dmaId>" +
          "<start>2014-04-01T01:00:00+0100</start>" +
          "<end>2014-04-01T01:00:00+0100</end>" +
          "<timeStep>3600</timeStep>" +
   "</query>" +
   "</request>";

            /*
            return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
"<request>" +
    "<recordSet>" +
        "<elementId>Cabanas-C1</elementId>" +
        "<record>" +
            "<time>2015-04-01T00:00:00+0200</time>" +
            "<variable>" +
                "<name>flow</name>" +
                "<value>3539014,42</value>" +
            "</variable>" +
        "</record>" +
        "<record>" +
            "<time>2015-04-01T01:00:00+0200</time>" +
            "<variable>" +
                "<name>flow</name>" +
                "<value>3539055,42</value>" +
            "</variable>" +
        "</record>" +
        "<record>" +
            "<time>2015-04-01T02:00:00+0200</time>" +
            "<variable>" +
                "<name>flow</name>" +
                "<value>3539090,52</value>" +
            "</variable>" +
        "</record>" +
    "</recordSet>"+
"</request>";
             */
        }

        public static string Create()
        {
            return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
"<request>" +
    "<recordSet>" +
        "<elementId>CC</elementId>" +
        "<record>" +
            "<time>2014-04-15T00:00:00+01:00</time>" +
            "<variable name=\"flow\">" +
                "<value>1111,1</value>" +
            "</variable>" +
        "</record>" +
        "<record>" +
            "<time>2014-04-15T01:00:00+01:00</time>" +
            "<variable name=\"flow\">" +
                "<value>2222,22</value>" +
            "</variable>" +
        "</record>" +
        "<record>" +
            "<time>2014-04-15T02:00:00+01:00</time>" +
            "<variable name=\"flow\">" +
                "<value>3333,33</value>" +
            "</variable>" +
        "</record>" +
    "</recordSet>" +
"</request>";
        }
    }
}
