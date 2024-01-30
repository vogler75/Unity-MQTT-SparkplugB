using System;
using System.Collections.Generic;
using UnityEngine;
using Google.Protobuf;
using Org.Eclipse.Tahu.Protobuf;

namespace Rocworks.Mqtt.SparkplugB
{
    public class EdgeNodePublisher : MetricHandler
    {
        [Header("Configuration")]
        public string GroupId = "";
        public string EdgeNodeId = "";
        public string PrimaryHostId = "";
        public bool UseAlias = false;

        [Header("Status")]
        public bool ConnectFlag = false;
        public bool ConnectState = false;
        public bool PublishChangedDataFlag = false;

        private IMqttClientUnity _connection = null;
        private MqttClient _client = null;

        private ulong _seq = 0;
        private Payload.Types.Metric _bdSeq; // Birth/Deatch Sequence Number

        private const string REBIRTH = "Node Control/Rebirth";

        private HostState _hostStateNow = new HostState() { online = false, timestamp = 0 };
        private HostState _hostStateOld = new HostState() { online = false, timestamp = 0 };

        // Start is called before the first frame update
        void Start()
        {
            _client = GetComponent<MqttClient>();
            _connection = _client.GetConnection();
            if (_connection == null)
            {
                Debug.LogError("No connection found!");
            }

            if (EdgeNodeId == "")
            {
                EdgeNodeId = gameObject.name;
            }

            if (GroupId == "")
            {
                GroupId = Application.companyName;
            }

            // Initialize birth/death sequence number
            _bdSeq = new()
            {
                Name = "bdSeq",
                Datatype = (uint)DataType.Int64,
                LongValue = (ulong)0
            };

            // Initialize MQTT client
            _client.BirthTopic = "";
            _client.BirthMessage = "";
            _client.BirthQualityOfService = 0;
            _client.BirthRetain = false;

            _client.WillTopic = "";
            _client.WillMessage = "";
            _client.WillQualityOfService = 0;
            _client.WillRetain = false;

            // On Message Arrived Event
            _client.OnMessageArrived.AddListener(OnMessageArrived);

            // On Connected Event
            _client.OnConnected.AddListener(() => 
            {
                _connection.Subscribe(GetTopic(MessageType.NCMD), qos: 1);
                if (PrimaryHostId != "")
                {
                    // Birth message is sent when primary host changes state to online
                    _connection.Subscribe(GetPrimaryHostStateTopic(), qos: 1);
                }
                else
                {
                    SendBirthMessage();
                }
            });

            // Before Disconnected Event
            _client.BeforeDisconnected.AddListener(() =>
            {
                // Nothing to do
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
                StartNode();
            }
            else if (ConnectState == true && ConnectFlag == false)
            {
                StopNode();
            }
            if (ConnectState == true && PublishChangedDataFlag == true)
            {
                PublishChangedDataFlag = false; // Reset flag
                PublishChangedData();
            }
        }

        private string GetTopic(MessageType messageType)
        {
            return Common.NAMESPACE + "/" + GroupId + "/" + messageType.ToString() + "/" + EdgeNodeId;
        }

        private string GetPrimaryHostStateTopic()
        {
            return Common.NAMESPACE + "/" + MessageType.STATE.ToString() + "/" + PrimaryHostId;
        }

        public void StartNode(bool withConnect=true)
        {
            // Birth/Death sequence number
            _bdSeq.LongValue++;
            if (_bdSeq.LongValue > 255)
                _bdSeq.LongValue = (ulong)0;

            // Reset primary host state
            _hostStateNow = new HostState() { online = false, timestamp = 0 };
            _hostStateOld = new HostState() { online = false, timestamp = 0 };

            // Will Message
            Payload willMessage = new();
            willMessage.Metrics.Add(_bdSeq);
            _client.WillTopic = GetTopic(MessageType.NDEATH);
            _client.SetWillMessage(willMessage.ToByteArray());
            _client.WillRetain = false;
            _client.WillOnDisconnect = true;

            // Connect
            ConnectState = true;
            _connection.Connect();
        }

        private void SendBirthMessage()
        {
            // Birth Message
            Payload birthMessage = CreateBirthMessage();
            _connection.Publish(GetTopic(MessageType.NBIRTH), birthMessage.ToByteArray(), qos: 0, retain: false);
        }

        public void StopNode()
        {
            ConnectState = false;
            _connection.Disconnect();
        }

        public bool PublishChangedData()
        {
            if (PrimaryHostId != "" && !_hostStateNow.online) return false; // Primary host is offline, nothing to publish

            var changedMetrics = GetChangedMetrics();
            if (changedMetrics.Count == 0) return false; // Nothing to publish
            ClearChangedMetrics();

            if (++_seq > 255) _seq = 0;
            Payload message = new()
            {
                Seq = _seq,
                Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };
            foreach (var source in changedMetrics)
            {
                var metric = new Payload.Types.Metric();
                if (UseAlias)
                {
                    if (source.HasAlias) metric.Alias = source.Alias;
                    else throw new Exception("Metric has no alias!");
                }
                else
                {
                    if (source.HasName) metric.Name = source.Name;
                    else throw new Exception("Metric has no name!");
                }               
                CopyMetricValue(source, metric);
                message.Metrics.Add(metric);
            }
            _connection.Publish(GetTopic(MessageType.NDATA), message.ToByteArray());
            return true;
        }        

        private Payload CreateBirthMessage()
        {
            // Birth Message
            Payload birthMessage = new()
            {
                Seq = _seq,
                Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            // Add birth/death sequence number
            birthMessage.Metrics.Add(_bdSeq);

            // Rebirth Metric
            Payload.Types.Metric rebirth = new()
            {
                Name = REBIRTH,
                Datatype = (uint)DataType.Boolean,
                BooleanValue = false
            };
            birthMessage.Metrics.Add(rebirth);

            // Birth Metrics
            foreach (var metric in GetMetrics())
            {
                birthMessage.Metrics.Add(metric.Clone());
            }
            ClearChangedMetrics();
            return birthMessage;
        }

        private void OnMessageArrived(MqttMessage m)
        {
            if (m.GetTopic() == GetTopic(MessageType.NCMD))
            {
                OnCommandMessage(m);
            }
            else if (m.GetTopic() == GetPrimaryHostStateTopic())
            {
                OnPrimaryHostStateMessage(m);
            }
            else
            {
                Debug.LogError("Unknown topic received: " + m.GetTopic());
            }
        }

        private void OnCommandMessage(MqttMessage m)
        {
            var payload = Payload.Parser.ParseFrom(m.GetBytes());
            foreach (var source in payload.Metrics)
            {
                if (source.HasName && source.Name == REBIRTH)
                {
                    if (source.BooleanValue == true)
                    {
                        // Re-Birth Message
                        Payload birthMessage = CreateBirthMessage();
                        _connection.Publish(GetTopic(MessageType.NBIRTH), birthMessage.ToByteArray());
                    }
                }
                else
                {
                    UpdateMetricValue(source);
                    UpdateMetricObject(source);
                }
            }
        }

        private void OnPrimaryHostStateMessage(MqttMessage m)
        {
            try
            {
                var state = JsonUtility.FromJson<HostState>(m.GetString());

                _hostStateOld = _hostStateNow;
                _hostStateNow = new HostState() { online = state.online, timestamp = state.timestamp };

                if (_hostStateNow.timestamp  > _hostStateOld.timestamp)
                {
                    if (_hostStateNow.online && !_hostStateOld.online)
                    {
                        Debug.Log("Host application changed to online!");
                        SendBirthMessage();
                    }
                    if (!_hostStateNow.online && _hostStateOld.online)
                    {
                        Debug.Log("Host application changed to offline!");
                    }
                }
            } catch (Exception e)
            {
                Debug.LogError("Error parsing host state message: " + e.Message);
            }
        }        

    }
}