using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace TableDataAccess
{
    public class MeteorologicalSensorEntity : TableEntity
    {
        public MeteorologicalSensorEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public MeteorologicalSensorEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string ID { get; set; }
        public double XCoordinate { get; set; }
        public double YCoordinate { get; set; }
        public string Supplier { get; set; }
        public string Code { get; set; }
        public string Type { get; set; }

    }

    public class MeteorologicalProductEntity : TableEntity
    {
        public MeteorologicalProductEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public MeteorologicalProductEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string ID { get; set; }
        public string Name { get; set; }
        public double Unit { get; set; }

    }

    public class MeteorologicalAssignmentEntity : TableEntity
    {
        public MeteorologicalAssignmentEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public MeteorologicalAssignmentEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string ID { get; set; }
        public string SensorID { get; set; }
        public string ProductID { get; set; }

    }

    public class RecordData : TableEntity
    {
        public RecordData()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public RecordData(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string Id { get; set; }
        public string IdAssign { get; set; }
        public DateTime Date { get; set; }
        public string Simuation { get; set; }
        public float Value { get; set; }

    }

    public class RasterFields : TableEntity
    {
        public RasterFields()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public RasterFields(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string ID { get; set; }
        public double Width { get; set; }
        public double Height { get; set; }
        public string Top_Coordinates { get; set; }
        public string Left_Coordinates { get; set; }
        public string Projection { get; set; }
        public string Resolution { get; set; }
        public string Type { get; set; }

    }

    public class RasterRecordData : TableEntity
    {
        public RasterRecordData()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public RasterRecordData(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string ID { get; set; }
        public string ID_RasterFields { get; set; }
        public DateTime Date { get; set; }
        public string Simulation { get; set; }
        public string Value { get; set; }
    }



}
