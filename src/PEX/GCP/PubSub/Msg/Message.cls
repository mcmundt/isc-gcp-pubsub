Class Example.PEX.GCP.PubSub.Msg.Message Extends (%Persistent, %XML.Adaptor)
{

Property MessageID As %String(MAXLEN = 512);

Property PublishTime As %String(MAXLEN = 64);

Property StringData As %Stream.GlobalCharacter;

Property BinaryData As %Stream.GlobalBinary;

Property Attributes As array Of %String(MAXLEN = 8192);

Property IsBinary As %Boolean [ InitialExpression = "0" ];

Method GetDataStream() As %Stream.Object
{
	if ..IsBinary quit ..BinaryData
	quit ..StringData
}

}
