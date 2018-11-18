package sjsu.drifters.servicemix;

import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.Random;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;


public class JmsServiceMixApp {

	public static String queueOrTopic = "QUEUE";
	
	public static void main(String[] args) throws Exception {
		
		int i = 0;
		int sleep = 0;
		
		Random r = new Random();
				
		// Try a max for 10 iterations to send messages from Producer to Consumer
		while (++i <= 10) {

			// Using random methods to send messages - either QUEUE or TOPIC
			if (i % 2 == 1) {
				queueOrTopic = "QUEUE";
				sleep = 100;
			} else {
				queueOrTopic = "TOPIC";
				sleep = 25;				
			}
			
			/*
			 * Integer n is defined to generate following combinations
			 * Producer(Thread1) - Consumer(Thread1)
			 * Producer(Thread1), Producer(Thread2) - Consumer(Thread1), Consumer(Thread2)
			 */
			int n = r.ints(1, 3).findFirst().getAsInt();
			
			// Producer loop 
			for (int j = 1; j <= n; j++) {
				thread(new HelloWorldProducer(), false);
				Thread.sleep(sleep);
			}
			
			// Consumer loop 
			for (int j = 1; j <= n; j++) {
				thread(new HelloWorldConsumer(), false);
				Thread.sleep(sleep);
			}
			
			Thread.sleep(400);
			
		}

	}

	public static void thread(Runnable runnable, boolean daemon) {
		Thread brokerThread = new Thread(runnable);
		brokerThread.setDaemon(daemon);
		brokerThread.start();
	}

	public static class HelloWorldProducer implements Runnable {
		public void run() {
			try {

				if (queueOrTopic == "QUEUE") {
					
					// Create a ConnectionFactory
					ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("karaf", "karaf",
							"tcp://localhost:61616");
					
					// Create a Connection
					Connection activeMQConnection = activeMQConnectionFactory.createConnection();

					activeMQConnection.start();
					// Create a Session
					Session activeMQSession = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

					// Create the destination QUEUE
					Destination activeMQDestination = activeMQSession.createQueue("DRIFTERS.CREATEQUEUE");

					// Create a MessageProducer from the Session to the QUEUE
					MessageProducer activeMQProducer = activeMQSession.createProducer(activeMQDestination);
					activeMQProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

					// Create a messages
					String mqtext = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
					TextMessage mqmessage = activeMQSession.createTextMessage(mqtext);

					// Tell the producer to send the message
					System.out.println("Session: " + queueOrTopic + " Sent message: " + mqmessage.hashCode() + " : " + Thread.currentThread().getName());
					activeMQProducer.send(mqmessage);

					// Clean up
					activeMQSession.close();
					activeMQConnection.close();
					
				} else {
					
					Context ctx = new InitialContext();
					ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("ConnectionFactory");

	                // Create a Connection
	                Connection connection = connectionFactory.createConnection("karaf","karaf");
	                connection.start();
	 
	                // Create a Session
	                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	 
	                // Create the destination TOPIC
	                Destination destination = (Destination) ctx.lookup("CreateTopic");
	 
	                // Create a MessageProducer from the Session to the Topic or Queue
	                MessageProducer producer = session.createProducer(destination);
	                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
	 
	                // Create a messages
	                String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
	                TextMessage message = session.createTextMessage(text);
	                	 
	                // Tell the producer to send the message
	                System.out.println("Session: " + queueOrTopic + " Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
	                producer.send(message);
	 
	                // Clean up
	                session.close();
	                connection.close();
	                
	                
				}				


			} catch (Exception e) {
				System.out.println("Caught: " + e);
				e.printStackTrace();
			}
		}
	}

	public static class HelloWorldConsumer implements Runnable, ExceptionListener {
		public void run() {
			try {

				if (queueOrTopic == "QUEUE") {
					// Create a ConnectionFactory
					ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("karaf", "karaf",
							"tcp://localhost:61616");

					// Create a Connection
					Connection activeMQConnection = activeMQConnectionFactory.createConnection();
					activeMQConnection.start();

					activeMQConnection.setExceptionListener(this);

					// Create a Session
					Session activeMQSession = activeMQConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

					// Create the destination (Topic or Queue)
					Destination activeMQDestination = activeMQSession.createQueue("DRIFTERS.CREATEQUEUE");

					// Create a MessageConsumer from the Session to the Topic or Queue
					MessageConsumer activeMQConsumer = activeMQSession.createConsumer(activeMQDestination);

					// Wait for a message
					Message activeMQMessage = activeMQConsumer.receive(2000);

					if (activeMQMessage instanceof TextMessage) {
						TextMessage mqtextMessage = (TextMessage) activeMQMessage;
						String mqtext = mqtextMessage.getText();
						System.out.println("Session: " + queueOrTopic + " Received: " + mqtext);
					} else {
						System.out.println("Session: " + queueOrTopic + " Received: " + activeMQMessage);
					}

					activeMQConsumer.close();
					activeMQSession.close();
					activeMQConnection.close();
					
				} else {
					
					Context ctx = new InitialContext();
					ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("ConnectionFactory");

	                // Create a Connection
	                Connection connection = connectionFactory.createConnection("karaf","karaf");
	                connection.start();
	 
	                // Create a Session
	                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	 
	                // Create the destination TOPIC
	                Destination destination = (Destination) ctx.lookup("CreateTopic");
	 
					// Create a MessageConsumer from the Session to the Topic
					MessageConsumer consumer = session.createConsumer(destination);

					// Wait for a message
					Message message = consumer.receive(2000);

					if (message instanceof TextMessage) {
						TextMessage textMessage = (TextMessage) message;
						String text = textMessage.getText();
						System.out.println("Session: " + queueOrTopic + " Received: " + text);
					} else {
						System.out.println("Session: " + queueOrTopic + " Received: " + message);
					}
	 
	                consumer.close();
	                session.close();
	                connection.close(); 
					
				}

			} catch (Exception e) {
				System.out.println("Caught: " + e);
				e.printStackTrace();
			}
		}

		public synchronized void onException(JMSException ex) {
			System.out.println("JMS Exception occured.  Shutting down client.");
		}
	}
}