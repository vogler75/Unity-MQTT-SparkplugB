using System;
using UnityEngine;
using Org.Eclipse.Tahu.Protobuf;

namespace Rocworks.Mqtt.SparkplugB
{
    public class EdgeMetric : MonoBehaviour
    {
        public DataType Type = DataType.Unknown;

        public string Name = "";
        public bool HasAlias = false;
        public ulong Alias = 0;
        public string Value = "";

        private IMetricHandler MetricHandler = null;

        private string OldValue = "";

        // Start is called before the first frame update
        void Start()
        {
            if (Name == null || Name == "")
                Name = gameObject.name;

            MetricHandler = GetComponentInParent<MetricHandler>();

            if (MetricHandler == null)
            {
                Debug.LogError("Metric must be a child of an MetricHandler!");
            }
            else
            {
                if (HasAlias)
                {
                    var metric = MetricHandler.AddMetric(Name, Alias, (DataType)Type);
                    MetricHandler.SetMetricObject(metric, this);
                }
                else
                {
                    var metric = MetricHandler.AddMetric(Name, (DataType)Type);
                    MetricHandler.SetMetricObject(metric, this);
                }
            }   
        }

        // Update is called once per frame
        void Update()
        {
            if (Value != OldValue)
            {
                SetFromString(Value);
            }
        }

        public void SetFromString(string value)
        {
            try
            {
                switch (Type)
                {
                    case DataType.Boolean:
                        MetricHandler.SetMetricValue(Name, bool.Parse(value));
                        break;
                    case DataType.Int8:
                    case DataType.Int16:
                    case DataType.Int32:
                        MetricHandler.SetMetricValue(Name, Int32.Parse(value));
                        break;
                    case DataType.Uint8:
                    case DataType.Uint16:
                    case DataType.Uint32:
                        MetricHandler.SetMetricValue(Name, UInt32.Parse(value));
                        break;
                    case DataType.Int64:
                        MetricHandler.SetMetricValue(Name, Int64.Parse(value));
                        break;
                    case DataType.Uint64:
                        MetricHandler.SetMetricValue(Name, UInt64.Parse(value));
                        break;
                    case DataType.Float:
                        MetricHandler.SetMetricValue(Name, float.Parse(value));
                        break;
                    case DataType.Double:
                        MetricHandler.SetMetricValue(Name, double.Parse(value));
                        break;
                    case DataType.String:
                    case DataType.Text:
                        MetricHandler.SetMetricValue(Name, value);
                        break;
                    default:
                        throw new Exception("Unsupported metric value type!");
                }
            }
            catch (Exception e)
            {
                Debug.LogError("Error setting metric value: " + e.Message + " (" + e.GetType().ToString() + ")");
            }
            finally
            {
                Value = OldValue = value;
            }
        }

        public void SetFromMetric(Payload.Types.Metric metric)
        {
            if (metric.HasAlias)
            {
                HasAlias = true;
                Alias = metric.Alias;
            }
            Type = (DataType)metric.Datatype;

            string value;
            switch (Type)
            {
                case DataType.Boolean:
                    value = metric.BooleanValue.ToString();
                    break;
                case DataType.Int8:
                case DataType.Int16:
                case DataType.Int32:
                    value = metric.IntValue.ToString();
                    break;
                case DataType.Uint8:
                case DataType.Uint16:
                case DataType.Uint32:
                    value = metric.IntValue.ToString();
                    break;
                case DataType.Int64:
                    value = metric.LongValue.ToString();
                    break;
                case DataType.Uint64:
                    value = metric.LongValue.ToString();
                    break;
                case DataType.Float:
                    value = metric.FloatValue.ToString();
                    break;
                case DataType.Double:
                    value = metric.DoubleValue.ToString();
                    break;
                case DataType.String:
                case DataType.Text:
                    value = metric.StringValue.ToString();
                    break;
                default:
                    throw new Exception("Unsupported metric value type!");
            }                        
            Value = OldValue = value;
        }

        public Payload.Types.Metric GetMetric()
        {
            return MetricHandler.GetMetric(Name);
        }   
    }
}
