using System;
using UnityEngine;
using Google.Protobuf;
using Org.Eclipse.Tahu.Protobuf;
using System.Collections.Generic;
using static Unity.VisualScripting.Member;
using System.Collections;

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
        public bool DebugMessages = false;

        private MqttClient _client = null;
        private IMqttClientUnity _connection = null;
        private long _birthTimestamp = 0;
        private ulong _seq = 0;
        private bool _subscribed = false;
        private bool _birthSent = false;

        private Dictionary<string, EdgeNodeConsumer> _edgeNodes = new();
        private Dictionary<string, EdgeDeviceConsumer> _edgeDevices = new();

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
                if (edgeNode.isActiveAndEnabled)
                    _edgeNodes.Add(edgeNode.GetName(), edgeNode);
            }

            // Find all child EdgeDevices
            foreach (EdgeDeviceConsumer edgeDevice in GetComponentsInChildren<EdgeDeviceConsumer>())
            {
                if (edgeDevice.isActiveAndEnabled)
                    _edgeDevices.Add(edgeDevice.GetName(), edgeDevice);
            }            

            // On Message Arrived Event
            _client.OnMessageArrived.AddListener(OnMessageArrived);

            // On Connected Event
            _client.OnConnected.AddListener(() =>
            {
                foreach (var edgeNode in _edgeNodes.Values)
                {
                    SubscribeEdgeNode(edgeNode);
                }
                foreach (var edgeDevice in _edgeDevices.Values)
                {
                    SubscribeEdgeDevice(edgeDevice);                    
                }
                _subscribed = true;
                _birthSent = false;
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
                _subscribed = false;
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
            if (_subscribed && PublishChangedDataFlag == true)
            {
                PublishChangedDataFlag = false;
                PublishChangedData();
            }       
            if (_subscribed && !_birthSent)
            {
                _birthSent = true;
                StartCoroutine(DoBirthAfterDelay());
            }
        }

        private IEnumerator DoBirthAfterDelay()
        {
            yield return new WaitForSeconds(1f); // wait 1 second
            SendBirthMessage();
        }

        private string GetTopic(MessageType messageType)
        {
            return Common.NAMESPACE + "/" + messageType.ToString() + "/" + HostApplicationId;
        }        
        
        public void StartApplication()
        {
            ConnectState = true;
            _birthTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            SetDeathMessage();
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

        private bool IsEdgeNodeOrDeviceTopic(string topic, out MessageType messageType, out string groupId, out string nodeId, out string deviceId)
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
                deviceId = "";
                return true;
            }
            else // "spBv1.0/myGroup/NDATA/myEdgeNode/myEdgeDevice"
            if (topicSegments.Length == 5 &&
                topicSegments[0] == Common.NAMESPACE &&
                (topicSegments[2] == MessageType.DBIRTH.ToString() ||
                 topicSegments[2] == MessageType.DDATA.ToString() ||
                 topicSegments[2] == MessageType.DDEATH.ToString()))
            {
                messageType = (MessageType)Enum.Parse(typeof(MessageType), topicSegments[2]);
                groupId = topicSegments[1];
                nodeId = topicSegments[3];
                deviceId = topicSegments[4];
                return true;
            } 
            else
            {
                Debug.LogError("Unknown topic format: " + topic);
                messageType = MessageType.UNKNOWN;
                groupId = "";
                nodeId = "";
                deviceId = "";
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

        public EdgeDeviceConsumer AddEdgeDevice(string groupId, string nodeId, string deviceId)
        {
            string name = groupId + "/" + nodeId + "/" + deviceId;
            if (_edgeDevices.TryGetValue(name, out var edgeDevice))
            {
                return edgeDevice;
            }
            else
            {
                GameObject go = new GameObject(name);
                go.transform.parent = transform;
                edgeDevice = go.AddComponent<EdgeDeviceConsumer>();
                edgeDevice.GroupId = groupId;
                edgeDevice.EdgeNodeId = nodeId;
                return edgeDevice;
            }            
        }        

        public void SubscribeEdgeNode(EdgeNodeConsumer edgeNode)
        {            
            _connection.Subscribe(edgeNode.GetTopic(MessageType.NBIRTH), qos: 0);
            _connection.Subscribe(edgeNode.GetTopic(MessageType.NDATA), qos: 0);
            _connection.Subscribe(edgeNode.GetTopic(MessageType.NDEATH), qos: 0);
        }

        public void SubscribeEdgeDevice(EdgeDeviceConsumer edgeDevice)
        {
            _connection.Subscribe(edgeDevice.GetTopic(MessageType.DBIRTH), qos: 0);
            _connection.Subscribe(edgeDevice.GetTopic(MessageType.DDATA), qos: 0);
            _connection.Subscribe(edgeDevice.GetTopic(MessageType.DDEATH), qos: 0);
        }        

        public void UnsubscribeEdgeNode(EdgeNodeConsumer edgeNode)
        {
            _connection.Unsubscribe(edgeNode.GetTopic(MessageType.NBIRTH));
            _connection.Unsubscribe(edgeNode.GetTopic(MessageType.NDATA));
            _connection.Unsubscribe(edgeNode.GetTopic(MessageType.NDEATH));
        }

        public void UnsubscribeEdgeDevice(EdgeDeviceConsumer edgeDevice)
        {
            _connection.Unsubscribe(edgeDevice.GetTopic(MessageType.DBIRTH));
            _connection.Unsubscribe(edgeDevice.GetTopic(MessageType.DDATA));
            _connection.Unsubscribe(edgeDevice.GetTopic(MessageType.DDEATH));
        }          

        private void OnMessageArrived(MqttMessage m)
        {
            if (IsEdgeNodeOrDeviceTopic(m.GetTopic(), out MessageType messageType, out string groupId, out string nodeId, out string deviceId))
            {
                if (deviceId == "") {
                    string name = groupId + "/" + nodeId;
                    if (_edgeNodes.TryGetValue(name, out var edgeNode))
                    {
                        SendMessageTo(m, messageType, name, edgeNode);
                    }
                    else 
                    {
                        Debug.LogError("EdgeNode not found: " + name);
                    }
                }
                else 
                {
                    string name = groupId + "/" + nodeId + "/" + deviceId;
                    if (_edgeDevices.TryGetValue(name, out var edgeDevice))
                    {
                        SendMessageTo(m, messageType, name, edgeDevice);
                    } 
                    else 
                    {
                        Debug.LogError("EdgeDevice not found: " + name);
                    }                    
                }
            } 
        }

        private void SendMessageTo(MqttMessage m, MessageType messageType, string name, EdgeConsumer edgeConsumer)
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
                if (DebugMessages) Debug.Log("Message arrived from " + name + " with type " + messageType.ToString());
                edgeConsumer.OnMessageArrived(messageType, payload);
            }
        }     

        public void PublishChangedData()
        {
            foreach (var edgeNode in _edgeNodes.Values)
            {
                PublishChangedDataTo(edgeNode);
            }
            foreach (var edgeDevice in _edgeDevices.Values)
            {
                PublishChangedDataTo(edgeDevice);
            }
        }

        private void PublishChangedDataTo(EdgeConsumer edgeNode)
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
                var topic = edgeNode.GetCmdTopic();
                _connection.Publish(topic, message.ToByteArray(), qos: 0, retain: false);
            }
        }
    }
}
