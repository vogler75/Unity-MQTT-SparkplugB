using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Rocworks.Mqtt.SparkplugB
{
    public class Common
    {
        public const string NAMESPACE = "spBv1.0";
    }    

    public enum MessageType {
        UNKNOWN,
        NBIRTH,
        NDEATH,
        DBIRTH,
        DDEATH,
        NDATA,
        DDATA,
        NCMD,
        DCMD,
        STATE
    }

    public class HostState
    {
        public bool online;
        public long timestamp;
    }
}