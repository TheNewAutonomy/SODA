using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace TableDataAccess
{
    public class TopNodeEntity : TableEntity
    {
        public TopNodeEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public TopNodeEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string NodeID { get; set; }
        public double XCoordinate { get; set; }
        public double YCoordinate { get; set; }

    }

    public class TopLineEntity : TableEntity
    {
        public TopLineEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public TopLineEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string LineID { get; set; }
        public string UpStreamNodeID { get; set; }
        public string DownStreamNodeID { get; set; }
        public string LineVertex { get; set; }

    }

    public class TopJunctionsEntity : TableEntity
    {
        public TopJunctionsEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public TopJunctionsEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string Description { get; set; }
        public double Elevation { get; set; }
        public string Zone { get; set; }
    }

    public class TopReservoirEntity : TableEntity
    {
        public TopReservoirEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public TopReservoirEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string Description { get; set; }
        public double Elevation { get; set; }
        public string Zone { get; set; }
    }

    public class TopTankEntity : TableEntity
    {
        public TopTankEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public TopTankEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string Description { get; set; }
        public double Elevation { get; set; }
        public double MinLevel { get; set; }
        public double MaxLevel { get; set; }
        public Enum TankType { get; set; }
        public double TankDiameter { get; set; }
        public double TankCurve { get; set; }
        public string Zone { get; set; }

    }

    public class TopPipesEntity : TableEntity
    {
        public TopPipesEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string Description { get; set; }
        public string Material { get; set; }
        public double Length { get; set; }
        public double Rugosity { get; set; }
    }

    public class TopPumpsEntity : TableEntity
    {
        public TopPumpsEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string Description { get; set; }
        public double Elevation { get; set; }
        public string Pump_Definition { get; set; }
        public string Zone { get; set; }
    }


    public class TopetypeOfValve : TableEntity
    {
        public TopetypeOfValve()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public TopetypeOfValve(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string TypeOfValve { get; set; }
        public double Elevation { get; set; }
        public double Diameter { get; set; }
        public double Setting { get; set; }
    }
}
