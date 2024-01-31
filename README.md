# SparkplugB for Unity

It needs the [MQTT for Unity](https://assetstore.unity.com/packages/tools/network/mqtt-for-unity-265888) Asset  from the Asset-Store.  

You can find a short demo and an explanatory video [here](https://youtu.be/rZRAuncq13I).

* Below a Host Application you can add GameObjects with the "Edge Node Consumer" Component. The Host Application will connect to this Edge Nodes and receive data from them. The Metrics will be created automatically when data is received.  

* Below a Edge Node Publisher you can add GameObjects with the "Edge Metric" Component. This will define the metrics of the Edge Node. You can add metrics to a Edge Node also by code.  

* At the Host Application and at the Edge Node Publisher you will find a "Publish Changed Data Flag". When you set this flag to true, the changed data will be published. The flag will be reseted immediately after the data has been published.  

* Host-Application and Edge-Node functionality is implemented, Edge-Devices are not implemented.  

* Edge-Nodes do not store a history of value changes. And so, if the Primary Host Application of an Edge-Node is down and comes up again, only the last value is published in the Birth message.  

* The Edge Metric GameObject currently represent the current value as a string. It's converted from/to the origin datatype. But the Edge Node itself has the list of all metrics in the SparkplugB message format. You can use the GetMetric(name) function to get a metric with all its details.  

