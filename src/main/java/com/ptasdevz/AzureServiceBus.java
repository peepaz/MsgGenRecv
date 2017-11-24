package com.ptasdevz;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveQueueMessageResult;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AzureServiceBus {

    //Service Bus Connect String
    private static final String connectionString = "Endpoint=sb://cloudassignment34ed0.servicebus.windows.net/;" +
            "SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=hoRn1WyP/74Zj7kUYAGOX0Tp+o6NGy4/fLbl90alG+4=";

    private static QueueClient sendClient;
    private static QueueClient receiveClient;
    private static List<Integer> requestCoutner = Collections.synchronizedList(new ArrayList<Integer>());
    private static  AzureServiceBus azureServiceBus = new AzureServiceBus();
    private static ServiceBusContract service;
    private static int errorInjectionNumber = 500; //threshold for error msg
    private static final Gson GSON = new Gson();
    private static int timer = 0;
    private static int timerMax = 20 ;
    private static int sleepTime = 0;

    private static AzureStorage azureStorage;
    private static Queue<ProductEntity> productsTostore = new LinkedList<>();
    private static Queue<ProductEntity> productsTostoreWithErrors = new LinkedList<>();
//    private static List<ProductEntity> productsTostoreWithErrors = Collections.synchronizedList(new ArrayList<>());

    private AzureServiceBus(){

      Configuration config =
                ServiceBusConfiguration.configureWithSASAuthentication(
                        "cloudassignment34ed0",
                        "RootManageSharedAccessKey",
                        "P6TUMCQVFg8ZIG8Z5KiPAIFaAHzvTcX9g7n8fNYAbZ0=",
                        ".servicebus.windows.net"
                );
        service = ServiceBusService.create(config);
        azureStorage = AzureStorage.getInstance();


    };

    //============================Azure Service Bus Methods===============================
    /**
     * Populates an Azure Service Bus queue with messages.
     * @param msgCount Number of messages to send to an Azure Service Bus queue.
     * @param msgRatePerSecond Number of messages to send per second.
     * @param isVerbose
     */
    public  static void sendMessages(String queueName, int msgCount, int msgRatePerSecond, boolean isVerbose) {

        String [] products =  {"Financial Trap", "Vizo Television", "Playstation 4", "Surface Pro","Ab worker","Chain Saw" };
        List<CompletableFuture> tasks = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(msgRatePerSecond);

        try {
            final int[] c = new int [1];
            final int requestSize = msgCount;
            final int requestPerSecond = msgRatePerSecond;
            sendClient = new QueueClient(new ConnectionStringBuilder(connectionString, queueName), ReceiveMode.PEEKLOCK);
            System.out.println("Running...");
            final String timeStr = "\n\nStarted - Request sent: " + requestCoutner.size()  + " time: "+ getTimeStamp();
            final ProductEntityPool productEntityPool = new ProductEntityPool();

            while (true) {


                Thread.sleep(0,1);
                pool.execute(new Runnable() {

                    @Override
                    public void run() {
                        try {

                            boolean isError = false;
                            if (c[0] % errorInjectionNumber == 0) isError = true;
                            final String messageId = Integer.toString(c[0]);

                            ProductEntity productEntity = (ProductEntity) productEntityPool.checkOut();
                            productEntity.setTransactionID(String.valueOf(c[0]));
                            productEntity.setUserId("cust_"+Math.random()*1000);
                            productEntity.setSellerID("user_"+Math.random()*1000);
                            productEntity.setProductName(products[c[0] % products.length]);

                            //omit sale attribute to simulate error
                            if (!isError) {
                                productEntity.setSalePrice(String.valueOf(Math.random() * 10000));
                            }
                            productEntity.setTransactionDate(String.valueOf(getTimeStamp()));
                            productEntityPool.checkIn(productEntity);

                            Message message = new Message(GSON.toJson(productEntity, ProductEntity.class).getBytes(UTF_8));

                            message.setContentType("application/json");
                            message.setLabel("products");
                            message.setMessageId(messageId);
                            message.setTimeToLive(Duration.ofMinutes(1440*10));

                            //simulate missing sale attribute error
                            if (isError){
                                HashMap <String,String> errorMsg = new HashMap<>();
                                errorMsg.put("errorMsg","missing sale attribute");
                                message.setProperties(errorMsg);
                            }
                            String id = message.getMessageId();
                            sendClient.sendAsync(message).thenRunAsync(() -> {
                                if (isVerbose) System.out.printf("\n\tMessage acknowledged: Id = %s", id);
                                requestCoutner.add(1);
                                tasks.remove(sendClient);

                                if (requestCoutner.size() == requestSize) {
                                    System.out.println(timeStr);
                                    System.out.println("Finished - Request sent: " + requestCoutner.size() +
                                            " time: " + getTimeStamp());
                                    try {
                                        sendClient.close();
                                    } catch (ServiceBusException e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                            message = null;



                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }
                });

                if (c[0] == requestSize-1) {
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

    private static void switchSleepTime() {
        if (sleepTime == 0) sleepTime = 1;
        else sleepTime = 0;
    }

    /**
     *  Get messages from an Azure Service Bus queue.
     * @param queueName The name of the Azure Service Bus queue on which to register for processing messages.
     * @param recvMsgRate The number of messages read concurrently from a queue.
     * @param isVerbose
     * @throws Exception
     */
    public static void receiveMessages(String queueName, int recvMsgRate, boolean isVerbose) throws Exception {
        startCountDownTimer();
        startServiceToInsert();


        boolean isQueueExist = isQueueExist(queueName);
        if (isQueueExist) {
            receiveClient = new QueueClient(new ConnectionStringBuilder(connectionString, queueName), ReceiveMode.PEEKLOCK);
            registerReceiver(receiveClient,recvMsgRate, isVerbose);
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
     * @param isVerbose
     * @throws Exception
     */
    private static void registerReceiver(QueueClient queueClient, int recvMsgRate, boolean isVerbose) throws Exception {

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
                   ProductEntity product = GSON.fromJson(new String(body, UTF_8), ProductEntity.class);
                   HashMap<String,String> error = (HashMap<String, String>) message.getProperties();

                   if (error != null) { //message does not contain an error
                       product.setErrorMsg(error.get("errorMsg"));
                       productsTostoreWithErrors.add(product);

                   }else {
                       productsTostore.add(product);
                   }

                   if (isVerbose) {
                       System.out.println(product.toString());
                   }
               }
//               return CompletableFuture.completedFuture(null);
               return CompletableFuture.completedFuture(null);
           }


           // callback invoked when the message handler has an exception to report
           public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
               System.out.printf(exceptionPhase + "-" + throwable.getMessage());
           }
       },
                //concurrent call, messages are auto-completed, auto-renew duration
                new MessageHandlerOptions(recvMsgRate, true, Duration.ofMinutes(5)));
        System.out.println("Running...");

    }
    public static void startServiceToInsert(){

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {

                System.out.println(productsTostore.size());

                // Your database code here
                if (productsTostore.size() > 0) {

                    JsonArray productArray = new JsonArray();
                    int size = productsTostore.size();
                    ProductEntity product;
                    for (int i = 0; i < size; i++) {
                        product = productsTostore.poll();
                        if (product != null) {
                            productArray.add(GSON.toJson(product));
                        }
                        else {
//                            System.out.println(productsTostore);
//                            System.out.println(product);
                        }
//                                    size--;
                    }
                    String productTosStoreJsonListStr = GSON.toJson(productArray);
                    AzureStorage.getInstance().insertProductBlob(productTosStoreJsonListStr, AzureStorage.productContainer);
                }

                if (productsTostoreWithErrors.size() > 0) {

                    JsonArray productArrayWithErrs = new JsonArray();
                    int size = productsTostoreWithErrors.size();
                    ProductEntity product;
                    for (int i = 0; i < size; i++) {
                        product = productsTostoreWithErrors.poll();
                        if (product != null) {
                            productArrayWithErrs.add(GSON.toJson(product));
                        }
//                                    size--;
                    }
                    String productTosStoreWithErrJsonListStr = GSON.toJson(productArrayWithErrs);
                    AzureStorage.getInstance().insertProductBlob(productTosStoreWithErrJsonListStr, AzureStorage.failStoreContainer);
                }
            }
        }, 30*1000, 30*1000);

    }

    public static  int getTimer(){
        return timer;
    }
    private static void startCountDownTimer(){

        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {

                int counter = 1;
                while (true) {

                    try {
                        timer =  timerMax - (counter % timerMax);
                        counter++;
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        th.start();

    }


    //============================Azure Service Bus Methods End===============================
    private static Timestamp getTimeStamp(){
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        return timestamp;
    }
    public static AzureServiceBus getInstance(){
        return azureServiceBus;
    }
}
