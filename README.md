# isc-gcp-pubsub

Interoperability components for receiving or publishing Google Cloud
Platform PubSub messages

## Overview

This project provides example interoperability components to receive
messages from and publish messages to Google Cloud Provider (GCP) PubSub
topics. It uses PEX and the GCP Java libraries to connect to GCP PubSub.

For more information:

IRIS PEX\
<https://docs.intersystems.com/irislatest/csp/docbook/DocBook.UI.Page.cls?KEY=EPEX>

<https://learning.intersystems.com/course/view.php?id=1716>

GCP PubSub

<https://cloud.google.com/pubsub/docs/overview>

## Components

#### PEX.GCP.PubSub.InboundAdapter.java

Java PEX Inbound Adapter for the IRIS business service
PEX.GCP.PubSub.Service.cls. Receives messages for a GCP PubSub
subscription using GCP Java libraries and sends them to the Business
Service's OnProcessInput() method as a PEX.GCP.PubSub.Msg.Message.

#### PEX.GCP.PubSub.Service.cls

IRIS business service. Receives and processes PEX.GCP.PubSub.Msg.Message
messages from PEX.GCP.PubSub.InboundAdapter and sends them to one or
more other IRIS Business Hosts.

#### PEX.GCP.PubSub.OutboundAdapter.java

Java PEX Outbound Adapter for the IRIS business operation
PEX.GCP.PubSub.Operation. Receives PEX.GCP.PubSub.Msg.Message messages
from other business hosts in IRIS and uses GCP Java libraries to publish
them to a GCP PubSub topic.

#### PEX.GCP.PubSub.Operation.cls

IRIS business operation. Receives PEX.GCP.PubSub.Msg.Message messages
from other IRIS Business Hosts and sends them to
PEX.GCP.PubSub.OutboundAdapter for publishing to a GCP PubSub topic. The
operation will return a PEX.GCP.Msg.PublishResponse containing the
message ID assigned by GCP PubSub.

#### PEX.GCP.PubSub.Msg.Message.cls

IRIS message class for GCP PubSub messages. Message data is stored in
one of two %Stream properties (depending on message encoding) -- use
GetDataStream() to retrieve the appropriate stream. See GCP PubSub
documentation for more information about Attributes, OrderingKey,
PublishTime, and MessageID.

IsBinary indicates if message is encoded as text or binary. The
InboundAdapter will set IsBinary based on the Remote Setting
GCPTopicEncoding. When sending a message to the OutboundAdapter, set
this to true (1) when sending to topics that are configured with an
Encoding of BINARY in the GCP Topic configuration.

## Making use of the components

### Preparation

1.  This currently works with IRIS 2020.1 and greater

2.  Download the "deployment" folder from GitHub

3.  Import the included ObjectScript classes from classes.xml in the
    "objectscript" subfolder into the namespace containing the
    interoperability production

4.  Copy the included Java JAR files from the "jar" subfolder to the
    server: GCPPubSub.jar and the libraries folder

###  Adding a Business Service to the Production

1.  Add a Business Service using the class EnsLib.JavaGateway.Service to
    the Production or use an existing EnsLib.JavaGateway.Service.\
    <https://docs.intersystems.com/irislatest/csp/docbook/DocBook.UI.Page.cls?KEY=EJVG_instructions>\
    If it doesn't already have them, add intersystems-gateway-x.y.z.jar
    and intersystems-jdbc-x.y.z.jar to Additional Settings/Class Path.
    These JARs can be found under \<IRIS installation
    directory>/dev/java/lib/JDK18

2.  Add a Business Service using the class PEX.GCP.PubSub.Service

### 

### Configure settings for the PubSub service

1.  Under *Basic Settings*, configure *TargetConfigNames* to send to one
    or more Business Hosts.

2.  Under *Remote InboundAdapter*:

    -   Set *Remote Classname* to the name of the Inbound Adapter Java
        class: PEX.GCP.PubSub.InboundAdapter

    -   Set *Gateway Host* and *Gateway Port* to those used by the
        EnsLib.JavaGateway.Service that was configured earlier

    -   To *Gateway Extra CLASSPATH*, add references to the JAR files
        that were copied to the server. Note that if you expect to have
        multiple GCP services and/or operations in the production you
        can add these JAR references to the EnsLib.JavaGateway.Service
        Class Path instead:

        -   GCPPubSub.jar

        -   JARs found in the library folder. To save time, you can copy
            this from the included file classpath.txt -- note that you
            will need to first edit the entries in classpath.txt to
            point to the directory where you placed the library folder.

    -   Configure *Remote Settings.* This is a free text field, and
        should contain multiple Key=Value entries, one per line.\
        \
        For an explanation of the available options, view the class
        documentation for PEX.GCP.PubSub.Service. The critical items
        are:

        -   GCPCredentials

        -   GCPProjectID

        -   GCPSubscriptionID

### Adding a Business Operation to the Production

A GCP PEX Business Operation is configured similarly to a Business
Service.

### Sending a message to a GCP PubSub topic

-   Construct a PEX.GCP.PubSub.Msg.Message

-   Set IsBinary to 1 for binary content or leave it at the default of 0
    for JSON or text

-   Populate the data stream using GetDataStream().Write() or another
    stream method

-   Populate attributes

-   Populate OrderingKey if needed

-   Send the message to the GCP PubSub Business Operation
