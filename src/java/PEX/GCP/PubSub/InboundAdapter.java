package PEX.GCP.PubSub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.collect.Lists;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import com.intersystems.gateway.GatewayContext;
import com.intersystems.jdbc.IRIS;
import com.intersystems.jdbc.IRISObject;

import java.io.ByteArrayInputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class InboundAdapter extends com.intersystems.enslib.pex.InboundAdapter{
	Subscriber subscriber = null;
	ArrayBlockingQueue<MessageWrapper> messages = null;

	public String LogLevel = "";
	public String GCPCredentials = "";
	public String GCPProjectID = "";
	public String GCPSubscriptionID = "";
	public String GCPTopicEncoding = "";
	public String MessageClass = "";
	public String InternalQueueSize = "";
	
	private IRIS iris;
	private boolean isBinary = false;
	private Level LogLevelInt = Level.OFF;

	private static String DefaultMessageClass = "PEX.GCP.PubSub.Msg.Message";
	private static int chunkSize = 3 * 1024 * 1024;
	
	private enum Level {
		OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE
	}
		
	public void OnInit() throws Exception {
		InitializeLogging();
		
		LogMessage(Level.DEBUG, "OnInit", "entering");
		LogMessage(Level.DEBUG, "OnInit", "starting with config values:");
		LogMessage(Level.DEBUG, "OnInit", "	InternalQueueSize: [" + InternalQueueSize + "]");
		LogMessage(Level.DEBUG, "OnInit", "	GCPProjectID: [" + GCPProjectID + "]");
		LogMessage(Level.DEBUG, "OnInit", "	GCPSubscriptionID: [" + GCPSubscriptionID + "]");
		LogMessage(Level.DEBUG, "OnInit", "	GCPTopicEncoding: [" + GCPTopicEncoding + "]");
		LogMessage(Level.DEBUG, "OnInit", "	MessageClass: [" + MessageClass + "]");
		LogMessage(Level.DEBUG, "OnInit", "	LogLevel: [" + LogLevel + "]");

		iris = GatewayContext.getIRIS();

		int internalQueueSize = 0;
		try {
			internalQueueSize = Integer.parseInt(InternalQueueSize);
		} catch (NumberFormatException ex) {
			internalQueueSize=1000;
			LogMessage(Level.INFO, "OnInit", "No setting for InternalQueueSize, defaulting to 1000");
		}
		messages = new ArrayBlockingQueue<MessageWrapper>(internalQueueSize);	
		
		if (GCPTopicEncoding != null && GCPTopicEncoding.trim().equalsIgnoreCase("BINARY")) {
			isBinary = true;
			LogMessage(Level.INFO, "OnInit", "using BINARY encoding");
		} else {
			LogMessage(Level.INFO, "OnInit", "using JSON encoding");
		}

		if (MessageClass != null && MessageClass.trim().length() > 0) {
			MessageClass = MessageClass.trim();
		} else {
			MessageClass = DefaultMessageClass;
		}
		
		LogMessage(Level.INFO, "OnInit", "setting up receiver and subscription");
		
		CredentialsProvider credProv = () -> {
			ByteArrayInputStream credStream = new ByteArrayInputStream(GCPCredentials.trim().getBytes("UTF-8"));
			GoogleCredentials credentials = GoogleCredentials.fromStream(credStream).createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
			return credentials;
		};
		
		ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(GCPProjectID.trim(), GCPSubscriptionID.trim());		
		
		MessageReceiver receiver = new MessageReceiver() {
				public void receiveMessage(final PubsubMessage message, final AckReplyConsumer consumer) {
					MessageWrapper wrapper = new MessageWrapper();
					wrapper.message=message;
					wrapper.consumer=consumer;
						   
					messages.offer(wrapper); 
				}
		};
		
		// Start the subscriber
		subscriber = Subscriber.newBuilder(subscriptionName, receiver).setCredentialsProvider(credProv).build();
		subscriber.startAsync().awaitRunning();
		LogMessage(Level.INFO, "OnInit", "listening for messages on " + subscriptionName.toString());
		LogMessage(Level.DEBUG, "OnInit", "leaving");

	}
	
	@Override
	public void OnTask() throws Exception {		
		LogMessage(Level.DEBUG, "OnInit", "entering");
		
		while (true) {
			MessageWrapper wrapper=messages.poll(200,TimeUnit.MILLISECONDS);

			if (wrapper == null) {
				break;
			}
			
			LogMessage(Level.INFO, "OnTask", "pulled message from queue");
			
			IRISObject msgObj = (IRISObject)(iris.classMethodObject(MessageClass,"%New"));
			msgObj.set("IsBinary", isBinary);			
			msgObj.set("MessageID", wrapper.message.getMessageId());
			
			if (wrapper.message.getOrderingKey() != null && wrapper.message.getOrderingKey().length() > 0) {
				msgObj.set("OrderingKey", wrapper.message.getOrderingKey());
			}
			
			LocalDateTime publishTime = LocalDateTime.ofEpochSecond(wrapper.message.getPublishTime().getSeconds(), wrapper.message.getPublishTime().getNanos(), ZoneOffset.ofHours(0));
			msgObj.set("PublishTime",publishTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

			Map<String,String> attMap = wrapper.message.getAttributesMap();
			if (attMap.size() > 0) {
				LogMessage(Level.DEBUG, "OnTask", "copying attributes");
				IRISObject attrArrObj = (IRISObject)msgObj.getObject("Attributes");
				attMap.forEach(new BiConsumer < String, String > () {
					public void accept(String k, String v) {
						attrArrObj.invoke("SetAt", v, k);
					}
				});
			}
			
			IRISObject strmObj = (IRISObject)msgObj.invokeObject("GetDataStream");

			LogMessage(Level.DEBUG, "OnTask", "inserting message data");
			int writeCount=0;
			
			if (isBinary) {
				while (writeCount < wrapper.message.getData().size()) {
					int lastByte=writeCount + chunkSize;
					if (lastByte >= wrapper.message.getData().size()) {
						lastByte=wrapper.message.getData().size();
					}

					byte[] msgChunk = wrapper.message.getData().substring(writeCount, lastByte).toByteArray();

					LogMessage(Level.DEBUG, "OnTask", "inserting bytes " + writeCount + "-" + lastByte);

					strmObj.invoke("Write", msgChunk);
					writeCount += msgChunk.length;

					LogMessage(Level.TRACE, "OnTask", "after stream Write()");
				}
			} else {
				String msgData=wrapper.message.getData().toStringUtf8();
				while (writeCount < msgData.length()) {
					int lastChar=writeCount + chunkSize;
					if (lastChar >= msgData.length()) {
						lastChar=msgData.length();
					}
					
					String msgChunk=msgData.substring(writeCount, lastChar);

					LogMessage(Level.DEBUG, "OnTask", "inserting bytes " + writeCount + "-" + lastChar);
					
					strmObj.invoke("Write", msgChunk);
					writeCount += msgChunk.length();
				}
			}
		
			LogMessage(Level.TRACE, "OnTask", "before processinput");
			LogMessage(Level.DEBUG, "OnTask", "calling ProcessInput()");
			BusinessHost.ProcessInput(msgObj);
			LogMessage(Level.TRACE, "OnTask", "after processinput");

			// Don't send an ack to GCP until the message has reached the business service
			wrapper.consumer.ack();
			LogMessage(Level.INFO, "OnTask", "acked msg id: " + wrapper.message.getMessageId());
		}
		LogMessage(Level.DEBUG, "OnTask", "leaving");
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

	public void OnTearDown() throws Exception {
		LogMessage(Level.DEBUG, "OnTearDown", "entering");
		LogMessage(Level.INFO, "OnTearDown", "stopping subscriber");
		subscriber.stopAsync();
		LogMessage(Level.TRACE, "OnTearDown", "after stopAsync");
		LogMessage(Level.DEBUG, "OnTearDown", "leaving");
	}

	private class MessageWrapper {
		PubsubMessage message = null;
		AckReplyConsumer consumer = null;
	}
}
