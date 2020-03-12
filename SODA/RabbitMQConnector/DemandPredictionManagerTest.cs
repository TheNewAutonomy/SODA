namespace RabbitMQConnector
{
    public class DemandPredictionManagerTest
    {
        public static string Create1()
        {
            return "<request>" +
                                    "<method>" +
                                        "create" +
                                    "</method>" +
                                    "<query>" +
                                        "<Dma>" +
                                            "1" +
                                        "</Dma>" +
                                        "<datetimeofprediction>" +
                                            "01/05/2015 21:16:02" +
                                        "</datetimeofprediction>" +
                                        "<datetimepredictioncreated>" +
                                            "01/01/2015 21:16:02" +
                                        "</datetimepredictioncreated>" +
                                        "<data>" +
                                            "1.5" +
                                        "</data>" +
                                        "<uncertainty>" +
                                            "0.3" +
                                        "</uncertainty>" +
                                    "</query>" +
                               "</request>";
        }

        public static string Create2()
        {
            return "<request>" +
                                    "<method>" +
                                        "create" +
                                    "</method>" +
                                    "<query>" +
                                        "<Dma>" +
                                            "1" +
                                        "</Dma>" +
                                        "<DateTimeOfPrediction>" +
                                            "08/05/2015 21:16:02" +
                                        "</DateTimeOfPrediction>" +
                                        "<DateTimePredictionCreated>" +
                                            "01/01/2015 21:16:02" +
                                        "</DateTimePredictionCreated>" +
                                        "<data>" +
                                            "2.52" +
                                        "</data>" +
                                        "<Uncertainty>" +
                                            "0.11" +
                                        "</Uncertainty>" +
                                    "</query>" +
                               "</request>";
        }

        public static string Create3()
        {
            return "<request>" +
                                    "<method>" +
                                        "create" +
                                    "</method>" +
                                    "<query>" +
                                        "<Dma>" +
                                            "1" +
                                        "</Dma>" +
                                        "<DateTimeOfPrediction>" +
                                            "15/05/2015 21:16:02" +
                                        "</DateTimeOfPrediction>" +
                                        "<DateTimePredictionCreated>" +
                                            "01/01/2015 21:16:02" +
                                        "</DateTimePredictionCreated>" +
                                        "<data>" +
                                            "2" +
                                        "</data>" +
                                        "<Uncertainty>" +
                                            "1.15" +
                                        "</Uncertainty>" +
                                    "</query>" +
                               "</request>";
        }

        public static string Create4()
        {
            return "<request>" +
                                    "<method>" +
                                        "create" +
                                    "</method>" +
                                    "<query>" +
                                        "<Dma>" +
                                            "1" +
                                        "</Dma>" +
                                        "<DateTimeOfPrediction>" +
                                            "15/05/2015 21:16:02" +
                                        "</DateTimeOfPrediction>" +
                                        "<DateTimePredictionCreated>" +
                                            "01/01/2015 21:16:02" +
                                        "</DateTimePredictionCreated>" +
                                        "<data>" +
                                            "8.5" +
                                        "</data>" +
                                        "<Uncertainty>" +
                                            "1.45" +
                                        "</Uncertainty>" +
                                    "</query>" +
                               "</request>";
        }

        public static string Create5()
        {
            return "<request>" +
                                    "<method>" +
                                        "create" +
                                    "</method>" +
                                    "<query>" +
                                        "<Dma>" +
                                            "1" +
                                        "</Dma>" +
                                        "<DateTimeOfPrediction>" +
                                            "08/05/2015 21:16:02" +
                                        "</DateTimeOfPrediction>" +
                                        "<DateTimePredictionCreated>" +
                                            "15/01/2015 21:16:02" +
                                        "</DateTimePredictionCreated>" +
                                        "<data>" +
                                            "18.43" +
                                        "</data>" +
                                        "<Uncertainty>" +
                                            "4" +
                                        "</Uncertainty>" +
                                    "</query>" +
                               "</request>";
        }

        public static string Create6()
        {
            return "<request>" +
                                    "<method>" +
                                        "create" +
                                    "</method>" +
                                    "<query>" +
                                        "<Dma>" +
                                            "1" +
                                        "</Dma>" +
                                        "<DateTimeOfPrediction>" +
                                            "15/05/2015 21:16:02" +
                                        "</DateTimeOfPrediction>" +
                                        "<DateTimePredictionCreated>" +
                                            "15/01/2015 21:16:02" +
                                        "</DateTimePredictionCreated>" +
                                        "<data>" +
                                            "17" +
                                        "</data>" +
                                        "<Uncertainty>" +
                                            "3.87" +
                                        "</Uncertainty>" +
                                    "</query>" +
                               "</request>";
        }

        public static string Create7()
        {
            return "<request>" +
                                    "<method>" +
                                        "create" +
                                    "</method>" +
                                    "<query>" +
                                        "<Dma>" +
                                            "2" +
                                        "</Dma>" +
                                        "<DateTimeOfPrediction>" +
                                            "15/05/2015 21:16:02" +
                                        "</DateTimeOfPrediction>" +
                                        "<DateTimePredictionCreated>" +
                                            "15/01/2015 21:16:02" +
                                        "</DateTimePredictionCreated>" +
                                        "<data>" +
                                            "99" +
                                        "</data>" +
                                        "<Uncertainty>" +
                                            "18.88" +
                                        "</Uncertainty>" +
                                    "</query>" +
                               "</request>";
        }

        public static string Read()
        {
            return "<request>" +
                                    "<method>" +
                                        "Read" +
                                    "</method>" +
                                    "<query>" +
                                        "<Dma>" +
                                            "1" +
                                        "</Dma>" +
                                        "<startdatetime>" +
                                            "01/05/2015 09:00:00" +
                                        "</startdatetime>" +
                                        "<enddatetime>" +
                                            "25/05/2015 09:00:00" +
                                        "</enddatetime>" +
                                        "<DateTimePredictionCreated>" +
                                            "02/01/2015 21:16:02" +
                                        "</DateTimePredictionCreated>" +
                                    "</query>" +
                               "</request>";
        }
    }
}
