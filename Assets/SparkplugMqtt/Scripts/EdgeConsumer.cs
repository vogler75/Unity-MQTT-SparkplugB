using Org.Eclipse.Tahu.Protobuf;
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Rocworks.Mqtt.SparkplugB
{
    public abstract class EdgeConsumer : MetricHandler
    {
        abstract public string GetName();
        abstract public string GetTopic(MessageType messageType);
        abstract public string GetCmdTopic();
        abstract public bool GetCreateObjects();

        abstract public void OnMessageArrived(MessageType messageType, Payload payload);

        protected void BirthMessage(Payload payload)
        {
            foreach (Payload.Types.Metric metric in payload.Metrics)
            {
                if (metric.HasName)
                {
                    if (!HasMetric(metric.Name))
                    {
                        AddMetric(metric);
                        if (GetCreateObjects()) {
                            NewMetricObject(metric);
                            UpdateMetricObject(metric);
                        }
                    }
                    else
                    {
                        if (GetCreateObjects()) 
                            UpdateMetricObject(metric);
                    }
                } 
                else
                {
                    throw new Exception("Metric has no name!");
                }
            }
        }

        protected void DataMessage(Payload payload)
        {
            foreach (Payload.Types.Metric metric in payload.Metrics)
            {
                UpdateMetricValue(metric);
                UpdateMetricObject(metric);
            }
        }
    }
}