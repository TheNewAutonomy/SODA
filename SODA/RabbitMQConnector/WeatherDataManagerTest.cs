namespace RabbitMQConnector
{
    public class WeatherDataManagerTest
    {
        public static string Read()
        {

            return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
"<recordSetRequest>" +
   "<recordSet>" +
      "<elementId>Tavira</elementId>" +
      "<record>" +
         "<from>2015-04-07T15:55:00+02:00</from>" +
         "<to>2015-4-7T14:00:00+00:00</to>" +
         "<variable>" +
            "<name>temperature</name>" +
            "<value>18.8</value>" +
         "</variable>" +
         "<variable>" +
            "<name>humidity</name>" +
            "<value>69</value>" +
         "</variable>" +
         "<variable>" +
            "<name>pressure</name>" +
            "<value>1015</value>" +
         "</variable>" +
         "<variable>" +
            "<name>wind_velocity</name>" +
            "<value>4.5</value>" +
         "</variable>" +
         "<variable>" +
            "<name>wind_direction</name>" +
            "<value>SE (143º)</value>" +
         "</variable>" +
         "<variable>" +
            "<name>solar_radiation</name>" +
            "<value>481.8</value>" +
         "</variable>" +
         "<variable>" +
            "<name>precipitation</name>" +
            "<value>0</value>" +
         "</variable>" +
      "</record>" +
      "<record>" +
         "<from>2015-04-07T16:00:00+02:00</from>" +
         "<to>2015-4-7T14:05:00+00:00</to>" +
         "<variable>" +
            "<name>temperature</name>" +
            "<value>18.3</value>" +
         "</variable>" +
         "<variable>" +
            "<name>humidity</name>" +
            "<value>69</value>" +
         "</variable>" +
         "<variable>" +
            "<name>pressure</name>" +
            "<value>1014</value>" +
         "</variable>" +
         "<variable>" +
            "<name>wind_velocity</name>" +
            "<value>5.2</value>" +
         "</variable>" +
         "<variable>" +
            "<name>wind_direction</name>" +
            "<value>SE (132º)</value>" +
         "</variable>" +
         "<variable>" +
            "<name>solar_radiation</name>" +
            "<value>386.2</value>" +
         "</variable>" +
         "<variable>" +
            "<name>precipitation</name>" +
            "<value>0</value>" +
         "</variable>" +
      "</record>" +
   "</recordSet>" +
"</recordSetRequest>";
        }
    }
}
