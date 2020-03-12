using System.Collections.Generic;
using System.Linq;

namespace RabbitMQConnector
{
    public class DMARelatedDataManager
    {
        public List<int> Indices;

        public string ElementId;

        public DMARelatedDataManager(RequestManager requestManager)
        {
            Indices = new List<int>();

            var indicesStr = requestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "indice").Value.Split(',').ToList();

            ElementId = requestManager.RootElements.FirstOrDefault(kvp => kvp.Key == "elementId").Value;

            foreach (var indice in indicesStr)
            {
                Indices.Add(int.Parse(indice));
            }
        }
    }
}
