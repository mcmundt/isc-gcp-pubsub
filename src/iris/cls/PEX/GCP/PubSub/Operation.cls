Class PEX.GCP.PubSub.Operation Extends Ens.BusinessOperation
{

Parameter ADAPTER = "EnsLib.PEX.OutboundAdapter";

Property Adapter As EnsLib.PEX.OutboundAdapter;

Parameter INVOCATION = "Queue";

Method PublishMessage(pRequest As Example.PEX.GCP.PubSub.Msg.Message, Output pResponse As Example.PEX.GCP.PubSub.Msg.PublishResponse) As %Status
{
	set pResponse=..Adapter.%DispatchMethod("OnMessage",pRequest)
	Quit $$$OK
}

XData MessageMap
{
<MapItems>
	<MapItem MessageType="Example.PEX.GCP.PubSub.Msg.Message"> 
		<Method>PublishMessage</Method>
	</MapItem>
</MapItems>
}

}
