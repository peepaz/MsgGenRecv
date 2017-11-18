
package com.ptasdevz;

import java.net.URISyntaxException;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.table.*;

/**
 * Message generator/receive: Permits the generation and retrieval of messages from  Azure Service Bus queues.
 * @author Jason Peters
 * @version 0.0.1
 */
public class Main {

    private static String queueName = "testqueue";
    public static int msgCount =1;
    public static int msgSendRatePerSecond =1;
    public static int msgRecvRateConcurrently =1;
    public static AzureServiceBus azureServiceBus;
    private static boolean isVerbose = false;

    public static void main(String[] args) {

        try {

            if (args.length >= 3) {

                switch (args[0]) {

                    case"send_messages":
                        azureServiceBus = AzureServiceBus.getInstance();
                        queueName = args[1];
                        azureServiceBus.createQueueIfNotExist(queueName);
                        msgCount = Integer.parseInt(args[2]);
                        msgSendRatePerSecond = Integer.parseInt(args[3]);
                        if (args.length == 5) {
                            if (args[4].equalsIgnoreCase("-v")) isVerbose = true;
                        }
                        azureServiceBus.sendMessages(queueName,msgCount,msgSendRatePerSecond,isVerbose);

                        break;

                    case "receive_messages":
                        queueName = args[1];
                        msgRecvRateConcurrently = Integer.parseInt(args[2]);
                        if (args.length == 4) {
                            if (args[3].equalsIgnoreCase("-v")) isVerbose = true;
                        }
                        azureServiceBus.receiveMessages(queueName,msgRecvRateConcurrently,isVerbose);

                        break;
                    case "":

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
}




