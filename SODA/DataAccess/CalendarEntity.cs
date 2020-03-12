using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace TableDataAccess
{
    public class CalendarEntity : TableEntity
    {
        public CalendarEntity()
            : base(DateTime.UtcNow.ToString("yyyy"),
                $"{DateTime.MaxValue.Ticks - DateTime.Now.Ticks:10}_{Guid.NewGuid()}")
        { }

        public CalendarEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        { }

        public string Year { get; set; }
        public string Description { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
    }
}
