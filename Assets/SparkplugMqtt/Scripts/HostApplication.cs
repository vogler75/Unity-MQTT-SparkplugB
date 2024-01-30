using System;
using UnityEngine;
using Google.Protobuf;
using Org.Eclipse.Tahu.Protobuf;
using System.Collections.Generic;
using static Unity.VisualScripting.Member;

namespace Rocworks.Mqtt.SparkplugB
{
    public class HostApplication : MonoBehaviour
    {
        [Header("Configuration")]
        public string HostApplicationId = "";

        [Header("Status")]
        public bool ConnectFlag = false;
        public bool ConnectState = false;
        public bool PublishChangedDataFlag = false;

        private IMqttClientUnity _connection = null;
        private MqttClient _client = null;
        private long _birthTimestamp = 0;
        private ulong _seq = 0;

        private Dictionary<string, EdgeNodeConsumer> _edgeNodes = new();

        // Start is called before the first frame update
        void Start()
        {
            _client = GetComponent<MqttClient>();
            _connection = _client.GetConnection();
            if (_connection == null)
            {
                Debug.LogError("No connection found!");
            }

            if (HostApplicationId == "")
            {
                HostApplicationId = gameObject.name;
            }

            // Find all child EdgeNodes
            foreach (EdgeNodeConsumer edgeNode in GetComponentsInChildren<EdgeNodeConsumer>())
            {
                _edgeNodes.Add(edgeNode.GroupId + "/" + edgeNode.EdgeNodeId, edgeNode);
            }

            // On Message Arrived Event
            _client.OnMessageArrived.AddListener(OnMessageArrived);

            // On Connected Event
            _client.OnConnected.AddListener(() =>
            {
                SendBirthMessage();
                foreach (var edgeNode in _edgeNodes.Values)
                {
                    SubscribeEdgeNode(edgeNode.GroupId, edgeNode.EdgeNodeId);
                }
            });

            // Before Disconnected Event
            _client.BeforeDisconnected.AddListener(() =>
            {
                SendDeathMessage();
            });

            // On Disconnected Event
            _client.OnDisconnected.AddListener(() =>
            {
                ConnectState = false;
            });
        }

        // Update is called once per frame
        void Update()
        {
            if (ConnectState == false && ConnectFlag == true)
            {
                StartApplication();
            }
            else if (ConnectState == true && ConnectFlag == false)
            {
                StopApplication();
            }
            if (ConnectState == true && PublishChangedDataFlag == true)
            {
                PublishChangedDataFlag = false;
                PublishChangedData();
            }
        }

        private string GetTopic(MessageType messageType)
        {
            return Common.NAMESPACE + "/" + messageType.ToString() + "/" + HostApplicationId;
        }

        private string GetNodeTopic(string groupId, string nodeId, MessageType messageType)
        {
            return Common.NAMESPACE + "/" + groupId + "/" + messageType.ToString() + "/" + nodeId;
        }
        
        public void StartApplication()
        {
            _birthTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            SetDeathMessage();
            ConnectState = true;
            _connection.Connect();
        }

        private void SetDeathMessage()
        {
            var willMessage = new HostState() { online = false, timestamp = _birthTimestamp };
            _client.WillTopic = GetTopic(MessageType.STATE);
            _client.WillMessage = JsonUtility.ToJson(willMessage);
            _client.WillRetain = true;
            _client.WillQualityOfService = 1;
            _client.WillOnDisconnect = false; // we do it manually on a graceful disconnect
        }

        private void SendDeathMessage()
        {
            long ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            HostState state = new() { online = false, timestamp = ts };
            string willMessage = JsonUtility.ToJson(state);
            _connection.Publish(GetTopic(MessageType.STATE), willMessage, qos: 1, retain: true);
        }

        private void SendBirthMessage()
        {
            var birthMessage = new HostState() { online = true, timestamp = _birthTimestamp };
            _connection.Publish(GetTopic(MessageType.STATE), JsonUtility.ToJson(birthMessage), qos: 1, retain: true);
        }

        public void StopApplication()
        {
            ConnectState = false;
            _connection.Disconnect();
        }

        private bool IsEdgeNodeTopic(string topic, out MessageType messageType, out string groupId, out string nodeId)
        {
            // Split the topic into segments
            string[] topicSegments = topic.Split('/');

            // "spBv1.0/myGroup/NDATA/myEdgeNode"
            if (topicSegments.Length == 4 &&
                topicSegments[0] == Common.NAMESPACE &&
                (topicSegments[2] == MessageType.NBIRTH.ToString() ||
                 topicSegments[2] == MessageType.NDATA.ToString() ||
                 topicSegments[2] == MessageType.NDEATH.ToString()))
            {
                messageType = (MessageType)Enum.Parse(typeof(MessageType), topicSegments[2]);
                groupId = topicSegments[1];
                nodeId = topicSegments[3];
                return true;
            }
            else
            {
                messageType = MessageType.UNKNOWN;
                groupId = "";
                nodeId = "";
                return false;
            }
        }

        public EdgeNodeConsumer AddEdgeNode(string groupId, string nodeId)
        {
            string name = groupId + "/" + nodeId;
            if (_edgeNodes.TryGetValue(name, out var edgeNode))
            {
                return edgeNode;
            }
            else
            {
                GameObject go = new GameObject(name);
                go.transform.parent = transform;
                edgeNode = go.AddComponent<EdgeNodeConsumer>();
                edgeNode.GroupId = groupId;
                edgeNode.EdgeNodeId = nodeId;
                return edgeNode;
            }            
        }

        public void SubscribeEdgeNode(string groupId, string nodeId)
        {
            _connection.Subscribe(GetNodeTopic(groupId, nodeId, MessageType.NBIRTH), qos: 0);
            _connection.Subscribe(GetNodeTopic(groupId, nodeId, MessageType.NDATA), qos: 0);
            _connection.Subscribe(GetNodeTopic(groupId, nodeId, MessageType.NDEATH), qos: 0);
        }

        public void UnsubscribeEdgeNode(string groupId, string nodeId)
        {
            _connection.Unsubscribe(GetNodeTopic(groupId, nodeId, MessageType.NBIRTH));
            _connection.Unsubscribe(GetNodeTopic(groupId, nodeId, MessageType.NDATA));
            _connection.Unsubscribe(GetNodeTopic(groupId, nodeId, MessageType.NDEATH));
        }

        private void OnMessageArrived(MqttMessage m)
        {
            if (IsEdgeNodeTopic(m.GetTopic(), out MessageType messageType, out string groupId, out string nodeId))
            {
                string name = groupId + "/" + nodeId;
                if (_edgeNodes.TryGetValue(name, out var edgeNode))
                {
                    Payload payload = null;
                    try
                    {
                        payload = Payload.Parser.ParseFrom(m.GetBytes());                        
                    }
                    catch (Exception e)
                    {
                        Debug.LogError("Error parsing message from topic " + m.GetTopic() + "\n" + e.StackTrace);
                    }
                    if (payload != null)
                    {
                        edgeNode.OnMessageArrived(messageType, payload);
                    }
                }                
            }   
        }

        public void SendCommandToNode(string groupId, string nodeId, Payload.Types.Metric metric)
        {
            SendCommandToNode(groupId, nodeId, new List<Payload.Types.Metric> { metric });
        }   

        public void SendCommandToNode(string groupId, string nodeId, List<Payload.Types.Metric> metrics)
        {
            Payload payload = new();
            payload.Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            payload.Metrics.Add(metrics);
            _connection.Publish(GetNodeTopic(groupId, nodeId, MessageType.NCMD), payload.ToByteArray(), qos: 0, retain: false);
        }

        public void PublishChangedData()
        {
            foreach (var edgeNode in _edgeNodes.Values)
            {
                var metrics = edgeNode.GetChangedMetrics();
                if (metrics.Count > 0)
                {
                    edgeNode.ClearChangedMetrics();
                    if (++_seq > 255) _seq = 0;
                    Payload message = new()
                    {
                        Seq = _seq,
                        Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };
                    foreach (var source in metrics)
                    {
                        var destination = new Payload.Types.Metric();
                        if (source.HasAlias) destination.Alias = source.Alias;
                        else if (source.HasName) destination.Name = source.Name;
                        else throw new Exception("Metric has no alias and no name!");
                        edgeNode.CopyMetricValue(source, destination);
                        message.Metrics.Add(destination);
                    }
                    var topic = GetNodeTopic(edgeNode.GroupId, edgeNode.EdgeNodeId, MessageType.NCMD);
                    _connection.Publish(topic, message.ToByteArray(), qos: 0, retain: false);                    
                }
            }
        }
    }
}
