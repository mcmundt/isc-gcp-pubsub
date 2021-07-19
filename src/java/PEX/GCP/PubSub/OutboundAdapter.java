package PEX.GCP.PubSub;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.intersystems.gateway.GatewayContext;
import com.intersystems.jdbc.IRIS;
import com.intersystems.jdbc.IRISObject;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class OutboundAdapter extends com.intersystems.enslib.pex.OutboundAdapter {
	public String LogLevel = "";
	public String GCPCredentials = "";
	public String GCPProjectID = "";
	public String GCPTopicID = "";
	public String GCPTopicEncoding = "";
	
	private Publisher publisher = null;
	private IRIS iris;
	private static int chunkSize = 3 * 1024 * 1024;
	private Level LogLevelInt = Level.OFF;
	
	private enum Level {
		OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE
	}	
	
	public void OnInit() throws Exception {
		InitializeLogging();
		
		LogMessage(Level.DEBUG,"OnInit","entering");
		LogMessage(Level.DEBUG,"OnInit","starting with config values:");
		LogMessage(Level.DEBUG,"OnInit","	GCPProjectID: [" + GCPProjectID + "]");
		LogMessage(Level.DEBUG,"OnInit","	GCPTopicID: [" + GCPTopicID + "]");
		LogMessage(Level.DEBUG,"OnInit","	GCPCredentials: [" + GCPCredentials + "]");
		LogMessage(Level.DEBUG,"OnInit","	GCPTopicEncoding: [" + GCPTopicEncoding + "]");
		
		iris = GatewayContext.getIRIS();
		
		CredentialsProvider credProv = () -> {
	    	ByteArrayInputStream credStream = new ByteArrayInputStream(GCPCredentials.getBytes("UTF-8"));
	        GoogleCredentials credentials = GoogleCredentials.fromStream(credStream).createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
	    	return credentials;
	    };

	    LogMessage(Level.INFO,"OnInit","setting up publisher");
	    TopicName topicName = TopicName.of(GCPProjectID.trim(), GCPTopicID.trim());
	    publisher = Publisher.newBuilder(topicName).setCredentialsProvider(credProv).build();

		LogMessage(Level.DEBUG,"OnInit","leaving");
	}

	public Object OnMessage(Object msgReq) throws Exception {
		LogMessage(Level.DEBUG,"OnMessage","entering");

		IRISObject msgObj = (IRISObject) msgReq;
		boolean isBinary = msgObj.getBoolean("IsBinary");
				
		IRISObject strmObj = (IRISObject)msgObj.invokeObject("GetDataStream");
		Long strmSize=strmObj.getLong("Size");
				
		ByteString msgData = ByteString.EMPTY;
		int bytesCopied = 0;

		while (bytesCopied < strmSize) {						
			if (isBinary) {
				byte[] msgChunk=strmObj.invokeBytes("Read",chunkSize);
				msgData = msgData.concat(ByteString.copyFrom(msgChunk));				
				LogMessage(Level.DEBUG,"OnMessage","copying chunk " + bytesCopied + "-" + (bytesCopied + msgChunk.length));
				bytesCopied += msgChunk.length;
			} else {
				String msgChunk=strmObj.invokeString("Read", chunkSize);
				msgData = msgData.concat(ByteString.copyFromUtf8(msgChunk));
				LogMessage(Level.DEBUG,"OnMessage","copying chunk " + bytesCopied + "-" + (bytesCopied + msgChunk.length()));
				LogMessage(Level.DEBUG,"OnMessage","msgData size: " + msgData.size());
				bytesCopied += msgChunk.length();
			}
		}

		PubsubMessage.Builder msgBuilder = PubsubMessage.newBuilder().setData(msgData);
		
		IRISObject attrArrObj = (IRISObject)msgObj.getObject("Attributes");
		if (attrArrObj.invokeLong("Count") > 0) {
			LogMessage(Level.DEBUG,"OnMessage","copying attributes");
			
			HashMap<String,String> attrMap = new HashMap<String,String>();

			String attrArrKey="";
			attrArrKey=attrArrObj.invokeString("Next",attrArrKey);
						
			while (attrArrKey != null && attrArrKey.length() > 0) {
				String attrArrVal=attrArrObj.invokeString("GetAt", attrArrKey);				
				attrMap.put(attrArrKey, attrArrVal);

				LogMessage(Level.TRACE,"OnMessage","	" + attrArrKey + "=" + attrArrVal);
				
				attrArrKey=attrArrObj.invokeString("Next",attrArrKey);	
			}
			
			msgBuilder=msgBuilder.putAllAttributes(attrMap);
		}

		LogMessage(Level.DEBUG,"OnMessage","create message");

		PubsubMessage pubsubMessage = msgBuilder.build();
		
		// Once published, returns a server-assigned message id (unique within the topic)
		LogMessage(Level.DEBUG,"OnMessage","publish message");
		ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
		String messageId = messageIdFuture.get();
		LogMessage(Level.DEBUG,"OnMessage","published message " + messageId);
		
		// Return record info
	    IRISObject pubResp = (IRISObject)(iris.classMethodObject("Example.PEX.GCP.PubSub.Msg.PublishResponse","%New",messageId));

		LogMessage(Level.DEBUG,"OnMessage","leaving");
	    return pubResp;
	}
	
	public void OnTearDown() throws Exception {
		LogMessage(Level.DEBUG,"OnTearDown","entering");
		LogMessage(Level.INFO,"OnTearDown","shutting down publisher");
		publisher.shutdown();
		LogMessage(Level.INFO,"OnTearDown","wait for publisher to terminate");
		publisher.awaitTermination(60, TimeUnit.SECONDS);
		LogMessage(Level.TRACE,"OnTearDown","after publisher termination or timeout");
		LogMessage(Level.DEBUG,"OnTearDown","leaving");
	}
	
	private void LogMessage(Level level, String method, String message) {		
		if (level.compareTo(LogLevelInt) <= 0) {
			message = "[" + level + "]	" + "[" + method + "]	" + message;
			
			switch (level) {
		        case OFF:
		        		break;
		        case FATAL:	
		        		 BusinessHost.LOGERROR(message);
		                 break;
		        case ERROR:	
		        		 BusinessHost.LOGERROR(message);
		                 break;
		        case WARN:	
		        		 BusinessHost.LOGWARNING(message);
		                 break;
		        case INFO:	
		        		 BusinessHost.LOGINFO(message);
		                 break;
		        case DEBUG:	
		        		 BusinessHost.LOGINFO(message);
		                 break;
		        case TRACE:	
		        		 BusinessHost.LOGINFO(message);
		                 break;
			}
		}		
	}
	
	private void InitializeLogging() {
		switch (LogLevel.trim().toUpperCase()) {
	        case "OFF":  LogLevelInt = Level.OFF;
	                 break;
	        case "FATAL":  LogLevelInt = Level.FATAL;
	                 break;
	        case "ERROR":  LogLevelInt = Level.ERROR;
	                 break;
	        case "WARN":  LogLevelInt = Level.WARN;
	                 break;
	        case "INFO":  LogLevelInt = Level.INFO;
	                 break;
	        case "DEBUG":  LogLevelInt = Level.DEBUG;
	                 break;
	        case "TRACE":  LogLevelInt = Level.TRACE;
	                 break;
	        default: LogLevelInt = Level.INFO;
	                 break;
		}
	}	
}
