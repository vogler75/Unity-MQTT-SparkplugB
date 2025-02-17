using Org.Eclipse.Tahu.Protobuf;
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Rocworks.Mqtt.SparkplugB
{
    public class EdgeDeviceConsumer : EdgeConsumer
    {        
        [Header("Configuration")]
        public string GroupId = "";
        public string EdgeNodeId = "";
        public string DeviceId = "";
        public bool CreateObjects = true;

        override public string GetName()
        {
            return GroupId + "/" + EdgeNodeId + "/" + DeviceId;
        }

        override public string GetTopic(MessageType messageType) {
            return Common.NAMESPACE + "/" + GroupId + "/" + messageType.ToString() + "/" + EdgeNodeId + "/" + DeviceId;
        }

        public override string GetCmdTopic()
        {
            return Common.NAMESPACE + "/" + GroupId + "/" + MessageType.DCMD + "/" + EdgeNodeId + "/" + DeviceId;
        }

        public override bool GetCreateObjects()
        {
            return CreateObjects;
        }

        // Start is called before the first frame update
        void Start()
        {           
            if (GroupId == "")
            {
                GroupId = Application.companyName;
            }

            if (EdgeNodeId == "")
            {
                EdgeNodeId = gameObject.name;
            }            

            if (DeviceId == "")
            {
                DeviceId = gameObject.name;
            }
        }

        // Update is called once per frame
        void Update()
        {

        }

        override public void OnMessageArrived(MessageType messageType, Payload payload)
        {
            switch (messageType)
            {                   
                case MessageType.DBIRTH: 
                    BirthMessage(payload); break;
                case MessageType.DDATA: 
                case MessageType.DDEATH: 
                    DataMessage(payload); break;                    
            }
        }
    }
}