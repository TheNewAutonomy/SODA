namespace RabbitMQConnector
{
    public class MeterManagerTest
    {
        public static string ReadList()
        {
            return
                
                "<request>" +
   "<query>" +
      "<elementId>Tavira</elementId>" +
   "</query>" +
"</request>";


            /*
            return "<request>" +
"<query>" +
"<start>12/04/2015 01:00:00</start>" +
"<end>13/04/2015 07:40:44</end>" +
"<elementid>LA</elementid>" +
"<timestep>3600</timestep>" +
"</query></request>";
            */
//            return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><request>   <query>      <elementid>Tavira</elementid>     </query></request>";
        //    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><request>   <query>      <start>12/04/2015 01:00:00</start>      <end>13/04/2015 07:40:44</end>      <elementid>LA</elementid>      <timestep>3600</timestep>   </query></request>";
/*            return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                        "<request>" +
                                "<query>" +
                                    "<start>" +
                                        "12/04/2015 01:00:00" +
                                    "</start>" +
                                    "<end>" +
                                        "13/04/2015 07:40:44" +
                                    "</end>" +
                                    "<elementid>" +
                                        "LA" +
                                    "</elementid>" +
                                "</query>" +
                            "</request>";*/
        }

        public static string Read()
        {
            return "<request>   <query>      <elementid>compteur1</elementid>          <start>2015-04-12T12:00:00.000+01:00</start>      <end>2015-04-15T18:00:00.000+01:00</end>      <timeStep>3600</timeStep>   </query> </request>";
//            return "<request><query><start>12/04/2015 01:00:00</start><end>15/04/2015 07:40:44</end><elementid>rc1</elementid><timestep>3600</timestep></query></request>";

            /*
            return "<request>" +
"<query>" +
"<start>12/04/2015 01:00:00</start>" +
"<end>15/04/2015 07:40:44</end>" +
"<elementid>rc1</elementid>" +
"<timestep>3600</timestep>" +
"</query></request>";
            */

//            return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><request>   <query>      <start>12/04/2015 01:00:00</start>      <end>13/04/2015 07:40:44</end>      <elementid>compteur1</elementid>      <timestep>3600</timestep>   </query></request>";
        //    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><request>   <query>      <start>12/04/2015 01:00:00</start>      <end>13/04/2015 07:40:44</end>      <elementid>LA</elementid>      <timestep>3600</timestep>   </query></request>";
/*            return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                        "<request>" +
                                "<query>" +
                                    "<start>" +
                                        "12/04/2015 01:00:00" +
                                    "</start>" +
                                    "<end>" +
                                        "13/04/2015 07:40:44" +
                                    "</end>" +
                                    "<elementid>" +
                                        "LA" +
                                    "</elementid>" +
                                "</query>" +
                            "</request>";*/
        }
    }
}
