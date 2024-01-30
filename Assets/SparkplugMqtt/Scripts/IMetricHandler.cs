using Org.Eclipse.Tahu.Protobuf;
using System;
using System.Collections.Generic;

namespace Rocworks.Mqtt.SparkplugB
{
    public interface IMetricHandler
    {
        bool HasMetric(string name);
        bool HasMetric(ulong alias);

        List<Payload.Types.Metric> GetMetrics();
        Payload.Types.Metric GetMetric(string name);
        Payload.Types.Metric GetMetric(ulong alias);
        public List<Payload.Types.Metric> GetChangedMetrics();
        public void ClearChangedMetrics();

        void AddMetric(Payload.Types.Metric metric);
        Payload.Types.Metric AddMetric(string name, DataType datatype);
        Payload.Types.Metric AddMetric(string name, ulong alias, DataType datatype);

        void RemoveMetric(string name);

        void SetMetricValue(string name, object value);
        void SetMetricValue(string name, object value, DateTimeOffset timestamp);
        void SetMetricMetadata(string name, Payload.Types.MetaData metadata);
        void SetMetricProperties(string name, Payload.Types.PropertySet properties);

        void CopyMetricValue(Payload.Types.Metric value, Payload.Types.Metric metric);

        void NewMetricObject(Payload.Types.Metric metric);
        void SetMetricObject(Payload.Types.Metric metric, EdgeMetric edgeMetric);
        void UpdateMetricObject(Payload.Types.Metric metric);
        void UpdateMetricValue(Payload.Types.Metric metric);
    }
}