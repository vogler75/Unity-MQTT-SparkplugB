using Org.Eclipse.Tahu.Protobuf;
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Rocworks.Mqtt.SparkplugB
{
    public class EdgeNodeConsumer : EdgeConsumer
    {
        [Header("Configuration")]
        public string GroupId = "";
        public string EdgeNodeId = "";
        public bool CreateObjects = true;

        override public string GetName()
        {
            return GroupId + "/" + EdgeNodeId;
        }

        override public string GetTopic(MessageType messageType) {
            return Common.NAMESPACE + "/" + GroupId + "/" + messageType.ToString() + "/" + EdgeNodeId;
        }

        public override string GetCmdTopic()
        {
            return Common.NAMESPACE + "/" + GroupId + "/" + MessageType.NCMD + "/" + EdgeNodeId;
        }

        public override bool GetCreateObjects()
        {
            return CreateObjects;
        }

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

        override public void OnMessageArrived(MessageType messageType, Payload payload)
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
    }
}