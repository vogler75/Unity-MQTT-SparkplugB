using Google.Protobuf;
using Org.Eclipse.Tahu.Protobuf;
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Rocworks.Mqtt.SparkplugB
{
    public abstract class MetricHandler : MonoBehaviour, IMetricHandler
    {
        private Dictionary<string, Payload.Types.Metric> _metricsByName = new();
        private Dictionary<ulong, string> _namesByAlias = new();
        private Dictionary<string, EdgeMetric> _metricObjectsByName = new();
        private HashSet<string> _changedMetrics = new();

        public bool HasMetric(string name)
        {
            return _metricsByName.ContainsKey(name);
        }

        public bool HasMetric(ulong alias)
        {
            return _namesByAlias.ContainsKey(alias);    
        }

        public List<Payload.Types.Metric> GetMetrics()
        {
            return new List<Payload.Types.Metric>(_metricsByName.Values);
        }

        public Payload.Types.Metric GetMetric(string name)
        {
            if (_metricsByName.ContainsKey(name))
            {
                return _metricsByName[name];
            }
            else
            {
                return null;
            }
        }

        public Payload.Types.Metric GetMetric(ulong alias)
        {
            if (_namesByAlias.TryGetValue(alias, out var name))
            {
                return _metricsByName[name];
            }
            else
            {
                return null;
            }
        }

        public List<Payload.Types.Metric> GetChangedMetrics()
        {
            var metrics = new List<Payload.Types.Metric>();
            foreach (var name in _changedMetrics)
            {
                metrics.Add(_metricsByName[name]);
            }
            return metrics;
        }

        public void ClearChangedMetrics() 
        { 
            _changedMetrics.Clear();
        }

        public Payload.Types.Metric AddMetric(string name, ulong alias, DataType datatype)
        {
            var metric = new Payload.Types.Metric();
            metric.Name = name;
            metric.Alias = alias;
            metric.Datatype = (uint)datatype;
            AddMetric(metric);
            return metric;
        }

        public Payload.Types.Metric AddMetric(string name, DataType datatype)
        {
            var metric = new Payload.Types.Metric();
            metric.Name = name;
            metric.Datatype = (uint)datatype;
            AddMetric(metric);
            return metric;
        }

        public void AddMetric(Payload.Types.Metric metric)
        {
            if (metric.HasName)
            {
                if (_metricsByName.ContainsKey(metric.Name))
                {
                    _metricsByName[metric.Name] = metric;
                }
                else
                {
                    _metricsByName.Add(metric.Name, metric);
                }
            }

            if (metric.HasAlias && metric.HasName)
            {
                if (_namesByAlias.ContainsKey(metric.Alias))
                {
                    _namesByAlias[metric.Alias] = metric.Name;
                }
                else
                {
                    _namesByAlias.Add(metric.Alias, metric.Name);
                }
            }
        }

        public void RemoveMetric(string name)
        {
            if (_metricsByName.ContainsKey(name))
            {
                _metricsByName.Remove(name);
            }
        }

        public void SetMetricValue(string name, object value)
        {
            SetMetricValue(name, value, DateTimeOffset.UtcNow);
        }

        public void SetMetricValue(string name, object value, DateTimeOffset timestamp)
        {
            if (_metricsByName.ContainsKey(name))
            {
                _changedMetrics.Add(name);
                var metric = _metricsByName[name];
                CopyValueToMetric(value, metric, (ulong)timestamp.ToUnixTimeMilliseconds());
            }
            else throw new Exception("Unknown metric name " + name + "!");
        }

        public void SetMetricProperties(string name, Payload.Types.PropertySet properties)
        {
            if (_metricsByName.ContainsKey(name))
            {
                _metricsByName[name].Properties = properties;
            }
            else throw new Exception("Unknown metric name " + name + "!");
        }

        public void SetMetricMetadata(string name, Payload.Types.MetaData metadata)
        {
            if (_metricsByName.ContainsKey(name))
            {
                _metricsByName[name].Metadata = metadata;
            }
            else throw new Exception("Unknown metric name " + name + "!");
        }

        public void NewMetricObject(Payload.Types.Metric metric)
        {
            var metricObject = new GameObject(metric.Name);
            var edgeMetric = metricObject.AddComponent<EdgeMetric>();
            metricObject.transform.parent = transform;
            _metricObjectsByName.Add(metric.Name, edgeMetric);
        }

        public void SetMetricObject(Payload.Types.Metric metric, EdgeMetric edgeMetric)
        {
            if (_metricObjectsByName.ContainsKey(metric.Name))
            {
                _metricObjectsByName[metric.Name] = edgeMetric;
            }
            else
            {
                _metricObjectsByName.Add(metric.Name, edgeMetric);
            }
        }

        public void UpdateMetricObject(Payload.Types.Metric metric)
        {            
            if (metric.HasName)
            {
                if (_metricObjectsByName.TryGetValue(metric.Name, out var destination))
                {
                    destination.SetFromMetric(metric);
                }
            } 
            else if (metric.HasAlias)
            {
                if (_namesByAlias.TryGetValue(metric.Alias, out var name))
                {
                    if (_metricObjectsByName.TryGetValue(name, out var destination))
                    {
                        destination.SetFromMetric(metric);
                    }
                }
            }
        }

        public void UpdateMetricValue(Payload.Types.Metric metric)
        {
            if (metric.HasName && _metricsByName.TryGetValue(metric.Name, out Payload.Types.Metric destination))
            {
                CopyMetricValue(metric, destination);
            }
            else if (metric.HasAlias && _namesByAlias.TryGetValue(metric.Alias, out string name))
            {
                if (_metricsByName.TryGetValue(name, out destination)) 
                {
                    CopyMetricValue(metric, destination);
                }                
            }   
            else
            {
                throw new Exception("Metric has no name or alias or is not registered!");
            }
        }

        public void CopyMetricValue(Payload.Types.Metric value, Payload.Types.Metric metric)
        {
            if (value.HasDatatype) metric.Datatype = value.Datatype;
            if (value.HasTimestamp) metric.Timestamp = value.Timestamp;
            else metric.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            switch (metric.Datatype)
            {
                case (uint)DataType.Boolean:
                    metric.BooleanValue = value.BooleanValue;
                    break;
                case (uint)DataType.Int8:
                case (uint)DataType.Uint8:
                case (uint)DataType.Int16:
                case (uint)DataType.Uint16:
                case (uint)DataType.Int32:
                case (uint)DataType.Uint32:
                    metric.IntValue = value.IntValue;
                    break;
                case (uint)DataType.Int64:
                case (uint)DataType.Uint64:
                    metric.LongValue = value.LongValue;
                    break;
                case (uint)DataType.Float:
                    metric.FloatValue = value.FloatValue;
                    break;
                case (uint)DataType.Double:
                    metric.DoubleValue = value.DoubleValue;
                    break;
                case (uint)DataType.Text:
                case (uint)DataType.String:
                    metric.StringValue = value.StringValue;
                    break;
                case (uint)DataType.Bytes:
                    metric.BytesValue = value.BytesValue;
                    break;
                default:
                    throw new Exception("Unhandled data type!");
            }
        }

        private void CopyValueToMetric(object value, Payload.Types.Metric metric, ulong timestamp)
        {
            metric.Timestamp = timestamp;
            switch (metric.Datatype)
            {
                case (uint)DataType.Boolean:
                    metric.BooleanValue = (bool)value;
                    break;
                case (uint)DataType.Int8:
                case (uint)DataType.Uint8:
                case (uint)DataType.Int16:
                case (uint)DataType.Uint16:
                case (uint)DataType.Int32:
                case (uint)DataType.Uint32:
                    metric.IntValue = (uint)value;
                    break;
                case (uint)DataType.Int64:
                case (uint)DataType.Uint64:
                    metric.LongValue = (ulong)value;
                    break;
                case (uint)DataType.Float:
                    metric.FloatValue = (float)value;
                    break;
                case (uint)DataType.Double:
                    metric.DoubleValue = (double)value;
                    break;
                case (uint)DataType.Text:
                case (uint)DataType.String:
                    metric.StringValue = (string)value;
                    break;
                case (uint)DataType.Bytes:
                    metric.BytesValue = (ByteString)value;
                    break;
                default:
                    throw new Exception("Unhandled data type!");
            }
        }
    }
}