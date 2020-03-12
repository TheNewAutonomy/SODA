using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace BLOBStorageMonitor
{
    public class FileInventoryEntity : TableEntity
    {
        public long LngFileLength { get; set; }
        public string Etag         { get; set; }
        public DateTime UploadDateTime { get; set; }
    }
}