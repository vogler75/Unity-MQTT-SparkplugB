using Org.Eclipse.Tahu.Protobuf;
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Rocworks.Mqtt.SparkplugB
{
    public class EdgeNodeConsumer : MetricHandler
    {
        [Header("Configuration")]
        public string GroupId = "";
        public string EdgeNodeId = "";

        // Start is called before the first frame update
        void Start()
        {           
            if (EdgeNodeId == "")
            {
                EdgeNodeId = gameObject.name;
            }

            if (GroupId == "")
            {
                GroupId = Application.companyName;
            }
        }

        // Update is called once per frame
        void Update()
        {

        }

        public void OnMessageArrived(MessageType messageType, Payload payload)
        {
            switch (messageType)
            {
                case MessageType.NBIRTH: 
                    BirthMessage(payload); break;
                case MessageType.NDATA: 
                case MessageType.NDEATH: 
                    DataMessage(payload); break;
            }
        }

        private void BirthMessage(Payload payload)
        {
            foreach (Payload.Types.Metric metric in payload.Metrics)
            {
                if (metric.HasName)
                {
                    if (!HasMetric(metric.Name))
                    {
                        AddMetric(metric);
                        NewMetricObject(metric);
                        UpdateMetricObject(metric);
                    }
                    else
                    {
                        UpdateMetricObject(metric);
                    }
                } 
                else
                {
                    throw new Exception("Metric has no name!");
                }
            }
        }

        private void DataMessage(Payload payload)
        {
            foreach (Payload.Types.Metric metric in payload.Metrics)
            {
                UpdateMetricValue(metric);
                UpdateMetricObject(metric);
            }
        }
    }
}