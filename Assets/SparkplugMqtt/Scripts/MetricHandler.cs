using Google.Protobuf;
using Org.Eclipse.Tahu.Protobuf;
using System;
using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;

namespace Rocworks.Mqtt.SparkplugB
{
    public abstract class MetricHandler : MonoBehaviour, IMetricHandler
    {
        private Dictionary<string, Payload.Types.Metric> _metricsByName = new();
        private Dictionary<ulong, string> _namesByAlias = new();
        private Dictionary<string, EdgeMetric> _metricObjectsByName = new();

        private Vector3 _lastNewPosition = new Vector3(0, 0, 0);

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
            foreach (var metric in _metricObjectsByName.Values)
            {
                if (metric.Changed)
                    metrics.Add(metric.GetMetric());
            }
            return metrics;
        }

        public void ClearChangedMetrics() 
        { 
            foreach (var metric in _metricObjectsByName.Values)
            {
                if (metric.Changed)
                    metric.Changed = false;
            }       
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
            var levels = metric.Name.Split('/');

            var metricObject = new GameObject(levels[levels.Length - 1]);
            var edgeMetric = metricObject.AddComponent<EdgeMetric>();
            edgeMetric.Name = metric.Name;       
            
            // create nodes for each level
            if (levels.Length > 1)
            {
                var current = transform;
                for (int i = 0; i < levels.Length - 1; i++)
                {
                    var parent = current.Find(levels[i]);
                    if (parent == null)
                    {
                        var node = new GameObject(levels[i]);
                        parent = node.transform;
                        parent.parent = current;
                    }
                    if (i == levels.Length - 2)
                    {
                        metricObject.transform.parent = parent;
                    }
                    current = parent;
                }
            }
            else
            {
                metricObject.transform.parent = transform;
            }

            // add text mesh pro
            if (_lastNewPosition.x == 0 && _lastNewPosition.y == 0 && _lastNewPosition.z == 0)
            {
                _lastNewPosition = transform.position;
            }
            
            if (_lastNewPosition.y > 100) {
                _lastNewPosition.y = 0;
                _lastNewPosition.x += 200f;
            } else {
                _lastNewPosition.y += 5f;            
            }
            metricObject.transform.position = _lastNewPosition;
            var textMetric = metricObject.AddComponent<TextMeshPro>();
            textMetric.GetComponent<RectTransform>().sizeDelta = new Vector2(200, 5);

            // add to dictionary
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
                    if (destination.TryGetComponent<TextMeshPro>(out var text))
                        destination.GetComponent<TextMeshPro>().text = destination.name+": "+destination.ValueAsString;
                }
            } 
            else if (metric.HasAlias)
            {
                if (_namesByAlias.TryGetValue(metric.Alias, out var name))
                {
                    if (_metricObjectsByName.TryGetValue(name, out var destination))
                    {
                        destination.SetFromMetric(metric);
                        if (destination.TryGetComponent<TextMeshPro>(out var text))
                            destination.GetComponent<TextMeshPro>().text = name+": "+destination.ValueAsString;                    }
                }
            }
        }

        public void UpdateMetricValue(Payload.Types.Metric metric)
        {
            if (metric.HasName)
            {
                if (_metricsByName.TryGetValue(metric.Name, out Payload.Types.Metric destination))
                    CopyMetricValue(metric, destination);
                //else Debug.LogWarning("Metric with name '"+metric.Name+"' is not registered!");
            }
            else if (metric.HasAlias)
            {
                if (_namesByAlias.TryGetValue(metric.Alias, out string name)) {
                    if (_metricsByName.TryGetValue(name, out Payload.Types.Metric destination)) 
                    {
                        CopyMetricValue(metric, destination);
                    }
                    //else Debug.LogWarning("Metric with alias '"+metric.Alias+"' and name '"+name+"' is not registered!");
                }
                //else Debug.LogWarning("Metric with alias '"+metric.Alias+"' is not registered!");
            }   
            else
            {
                throw new Exception("Metric has no name or alias!");
            }
        }

        public void CopyMetricValue(Payload.Types.Metric value, Payload.Types.Metric metric)
        {
            if (value.HasDatatype) metric.Datatype = value.Datatype;
            if (value.HasTimestamp) metric.Timestamp = value.Timestamp;
            else metric.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var type = (DataType)metric.Datatype;

            switch (type)
            {
                case DataType.Boolean:
                    metric.BooleanValue = value.BooleanValue;
                    break;
                case DataType.Int8:
                case DataType.Uint8:
                case DataType.Int16:
                case DataType.Uint16:
                case DataType.Int32:
                case DataType.Uint32:
                    metric.IntValue = value.IntValue;
                    break;
                case DataType.Int64:
                case DataType.Uint64:
                    metric.LongValue = value.LongValue;
                    break;
                case DataType.Float:
                    metric.FloatValue = value.FloatValue;
                    break;
                case DataType.Double:
                    metric.DoubleValue = value.DoubleValue;
                    break;
                case DataType.Text:
                case DataType.String:
                    metric.StringValue = value.StringValue;
                    break;
                case DataType.Bytes:
                    metric.BytesValue = value.BytesValue;
                    break;
                case DataType.Template:
                    metric.StringValue = value.TemplateValue.ToString();
                    break;
                default:
                    throw new Exception("Unhandled data type '" + type + "'!");
            }
        }

        private void CopyValueToMetric(object value, Payload.Types.Metric metric, ulong timestamp)
        {
            metric.Timestamp = timestamp;
            var type = (DataType)metric.Datatype;
            switch (type)
            {
                case DataType.Boolean:
                    metric.BooleanValue = (bool)value;
                    break;
                case DataType.Int8:
                case DataType.Int16:
                case DataType.Int32:
                    metric.IntValue = value as uint? ?? 0;
                    break;
                case DataType.Uint8:
                case DataType.Uint16:
                case DataType.Uint32:
                    metric.IntValue = (uint)value;
                    break;
                case DataType.Int64:
                    metric.LongValue = value as ulong? ?? 0;
                    break;
                case DataType.Uint64:
                    metric.LongValue = (ulong)value;
                    break;
                case DataType.Float:
                    metric.FloatValue = (float)value;
                    break;
                case DataType.Double:
                    metric.DoubleValue = (double)value;
                    break;
                case DataType.Text:
                case DataType.String:
                    metric.StringValue = (string)value;
                    break;
                case DataType.Bytes:
                    metric.BytesValue = (ByteString)value;
                    break;
                default:
                    throw new Exception("Unhandled data type '" + type + "'!");
            }
        }
    }
}