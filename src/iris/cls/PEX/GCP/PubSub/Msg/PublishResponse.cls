Class PEX.GCP.PubSub.Msg.PublishResponse Extends (%Persistent, %XML.Adaptor)
{

Property MessageID As %String(MAXLEN = 4096);

Method %OnNew(pMessageID As %String) As %Status [ Private, ServerOnly = 1 ]
{
	set ..MessageID=pMessageID
	Quit $$$OK
}

}
