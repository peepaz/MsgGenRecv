
package com.ptasdevz;

import com.google.gson.Gson;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Message generator/receive: Permits the generation and retrieval of messages from  Azure Service Bus queues.
 * @author Jason Peters
 * @version 0.0.1
 */
public class Main {

    private static List<Integer> requestCoutner = Collections.synchronizedList(new ArrayList<Integer>());
    private static final String connectionString = "Endpoint=sb://cloudassignment34ed0.servicebus.windows.net/;" +
            "SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=hoRn1WyP/74Zj7kUYAGOX0Tp+o6NGy4/fLbl90alG+4=";
    private static String queueName = "testqueue";
    public  static QueueClient sendClient;
    public  static QueueClient receiveClient;
    public static int msgCount =1;
    public static int msgSendRatePerSecond =1;
    public static int msgRecvRateConcurrently =1;
    private static boolean isVerbose = false;
    public  static  Configuration config =
            ServiceBusConfiguration.configureWithSASAuthentication(
                    "cloudassignment34ed0",
                    "RootManageSharedAccessKey",
                    "P6TUMCQVFg8ZIG8Z5KiPAIFaAHzvTcX9g7n8fNYAbZ0=",
                    ".servicebus.windows.net"
            );

    public static ServiceBusContract service;

    public static final Gson GSON = new Gson();

    public static void main(String[] args) {

        try {
            //register service bus config object
            service = ServiceBusService.create(config);

            if (args.length >= 3) {

                switch (args[0]) {

                    case"send_messages":
                        queueName = args[1];
                        createQueueIfNotExist(queueName);
                        msgCount = Integer.parseInt(args[2]);
                        msgSendRatePerSecond = Integer.parseInt(args[3]);
                        if (args.length == 5) {
                            if (args[4].equalsIgnoreCase("-v")) isVerbose = true;
                        }
                        sendMessages(msgCount,msgSendRatePerSecond);

                        break;

                    case "receive_messages":
                        queueName = args[1];
                        msgRecvRateConcurrently = Integer.parseInt(args[2]);
                        if (args.length == 4) {
                            if (args[3].equalsIgnoreCase("-v")) isVerbose = true;
                        }
                        receiveMessages(queueName,msgRecvRateConcurrently);

                        break;

                    default:
                        System.out.println("invalid_option");
                }

            }else {

            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    /**
     * Populates an Azure Service Bus queue with messages.
     * @param msgCount Number of messages to send to an Azure Service Bus queue.
     * @param msgRatePerSecond Number of messages to send per second.
     */
    private static void sendMessages(int msgCount, int msgRatePerSecond) {

        String [] products =  {"Financial Trap", "Vizo Television", "Playstation 4", "Surface Pro","Ab worker","Chain Saw" };
        List<CompletableFuture> tasks = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(msgRatePerSecond);

        try {
            final int[] c = new int [1];
            final int requestSize = msgCount;
            final int requestPerSecond = msgRatePerSecond;
            sendClient = new QueueClient(new ConnectionStringBuilder(connectionString, queueName), ReceiveMode.PEEKLOCK);

           final String timeStr = "\n\nStarted - Request sent: " + requestCoutner.size()  + " time: "+ getTimeStamp();

            while (true) {

                Thread.sleep(1000/requestPerSecond);
                pool.execute(new Runnable() {

                    @Override
                    public void run() {
//                        System.out.println("Request sent at time: " +getTimeStamp());
                        try {

                            final String messageId = Integer.toString(c[0]);
                            String msg = "{'TransactionID':'"+c[0]+"','UserId':'cust_"+Math.random()*1000+"','SellerID':'usr_"+Math.random()*1000
                                    +"','Product Name':'"+products[c[0] % products.length]+"','Sale Price':"+Math.random()*10000+",'Transaction Date':'"+getTimeStamp()+"'}";
                            Message message = new Message(GSON.toJson(msg, String.class).getBytes(UTF_8));
                            message.setContentType("application/json");
                            message.setLabel("products");
                            message.setMessageId(messageId);
                            message.setTimeToLive(Duration.ofMinutes(1440*10));
                            tasks.add(
                                    sendClient.sendAsync(message).thenRunAsync(() -> {
                                        if (isVerbose) System.out.printf("\n\tMessage acknowledged: Id = %s", message.getMessageId());
                                        requestCoutner.add(1);

                                        if (requestCoutner.size() == requestSize) {
                                            System.out.println(timeStr);
                                            System.out.println("Finished - Request sent: " + requestCoutner.size()  + " time: "+ getTimeStamp());
                                            try {
                                                sendClient.close();
                                            } catch (ServiceBusException e) {
                                                e.printStackTrace();
                                            }
                                        }
                                    }));


                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }
                });

                if (c[0] == requestSize) {
                    break;
                }
                c[0]++;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            pool.shutdown();
        }
    }

    /**
     *  Get messages from an Azure Service Bus queue.
     * @param queueName The name of the Azure Service Bus queue on which to register for processing messages.
     * @param recvMsgRate The number of messages read concurrently from a queue.
     * @throws Exception
     */
    public static void receiveMessages(String queueName,int recvMsgRate) throws Exception {

        boolean isQueueExist = isQueueExist(queueName);
        if (isQueueExist) {
            receiveClient = new QueueClient(new ConnectionStringBuilder(connectionString, queueName), ReceiveMode.PEEKLOCK);
            registerReceiver(receiveClient,recvMsgRate);
        }
        else System.out.printf("Queue: %s does not exist.",queueName);





    }

    /**
     * Creates a queue on the Azure Service Bus if it doesn't exists.
     * @param queueName The name of the Azure Service Bus queue to create if it doesn't exist
     */
    public static void createQueueIfNotExist(String queueName) {

        try {
            boolean isQueueExist = isQueueExist(queueName);
            if (!isQueueExist) {
                QueueInfo queueInfo = new QueueInfo(queueName);
                service.createQueue(queueInfo);
                Thread.sleep(500);
                System.out.println(queueName + " was created");
            }

        } catch (ServiceException e) {
            System.out.print("ServiceException encountered: ");
            System.out.println();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Checks if a queue exists on the Azure Service Bus
     * @param queueName The name of the Azure Service Bus queue
     * @return true if exist otherwise false
     * @throws ServiceException
     */
    private static boolean isQueueExist(String queueName) throws ServiceException {
        boolean isQueueExist = false;
        List<QueueInfo>
                listQueuesResult = service.listQueues().getItems();
        for (QueueInfo queueInfo :listQueuesResult) {
            String queueTitle = queueInfo.getEntry().getTitle();
            if (queueName.equalsIgnoreCase(queueTitle)) {
                isQueueExist = true;
                break;
            }
        }
        return isQueueExist;
    }

    public static void deleteQueueIfExist (String queueName){
        try {
            boolean isQueueExist = false;
            List<QueueInfo>
                    listQueuesResult = service.listQueues().getItems();
            for (QueueInfo queueInfo :listQueuesResult) {
                String queueTitle = queueInfo.getEntry().getTitle();
                if (queueName.equalsIgnoreCase(queueTitle)) {
                    isQueueExist = true;
                    break;
                }
            }
            if (isQueueExist) {
                service.deleteQueue(queueName);
                Thread.sleep(500);
                System.out.println(queueName + " was deleted");
            }
        } catch (ServiceException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void  getMessage(String queueName){

        try {
            ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
            opts.setReceiveMode(com.microsoft.windowsazure.services.servicebus.models.ReceiveMode.PEEK_LOCK);

            ReceiveQueueMessageResult queueMessageResult = service.receiveQueueMessage(queueName,opts);
            BrokeredMessage msg = queueMessageResult.getValue();
            byte[] b = new byte[2000];
            int numRead = msg.getBody().read(b);
            String s = null;
            while (-1 != numRead)
            {
                s = new String(b);
                s = s.trim();
                System.out.print(s);
                numRead = msg.getBody().read(b);
            }
            System.out.println(msg.getMessageId());

        } catch (ServiceException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Register a queue client to receive messages from an Azure Service Bus queue
     * @param queueClient - Queue client object
     * @param recvMsgRate - Number of messages to receive concurrently
     * @throws Exception
     */
    private static void registerReceiver(QueueClient queueClient, int recvMsgRate) throws Exception {

        // register the RegisterMessageHandler callback
        queueClient.registerMessageHandler(new IMessageHandler() {

           // callback invoked when the message handler loop has obtained a message
           public CompletableFuture<Void> onMessageAsync(IMessage message) {

               // receives message is passed to callback
               if (message.getLabel() != null &&
                       message.getContentType() != null &&
                       message.getLabel().contentEquals("products") &&
                       message.getContentType().contentEquals("application/json")) {

                   byte[] body = message.getBody();
                   String products = GSON.fromJson(new String(body, UTF_8), String.class);
                   if (isVerbose) {
                       System.out.println(products);
                   }

//                           System.out.printf(
//                                   "\n\t\t\t\tMessage received: \n\t\t\t\t\t\tMessageId = %s, \n\t\t\t\t\t\tSequenceNumber = %s," +
//                                           " \n\t\t\t\t\t\tEnqueuedTimeUtc = %s,\n\t\t\t\t\t\tExpiresAtUtc = %s, " +
//                                           "\n\t\t\t\t\t\tContentType = \"%s\",  \n\t\t\t\t\t\tContent: [ UserId = %s, Product Name = %s, Sale Price = %s ]\n",
//                                   message.getMessageId(),
//                                   message.getSequenceNumber(),
//                                   message.getEnqueuedTimeUtc(),
//                                   message.getExpiresAtUtc(),
//                                   message.getContentType(),
//                                   products != null ? products.get("UserId") : "",
//                                   products != null ? products.get("Product Name") : "",
//                                   products != null ? products.get("Sale Price") : "");
               }
               return CompletableFuture.completedFuture(null);
           }

                   // callback invoked when the message handler has an exception to report
                   public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                       System.out.printf(exceptionPhase + "-" + throwable.getMessage());
                   }
               },
        //concurrent call, messages are auto-completed, auto-renew duration
        new MessageHandlerOptions(recvMsgRate, true, Duration.ofMinutes(1)));
        System.out.println("Waiting for messages...");


    }

    private static Timestamp getTimeStamp(){
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        return timestamp;
    }
}


