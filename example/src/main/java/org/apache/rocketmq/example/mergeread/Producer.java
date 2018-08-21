package org.apache.rocketmq.example.mergeread;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;

/**
 * Created by wkj on 2018/8/20.
 */
public class Producer {
    public static String groupName = "MergeSort";
    public static String topic = "Merge";
    public static String brokerName = "broker-a";
    static int queueLength = 4;
    public static void main(String[] args) throws MQClientException, InterruptedException{
        DefaultMQProducer producer = new DefaultMQProducer(groupName);
        producer.setNamesrvAddr("localhost:9876");
        ArrayList<MessageQueue> queues = new ArrayList<>();
        for (int i= 0; i<queueLength; i++){
            MessageQueue q = new MessageQueue(topic, brokerName, i);
            queues.add(q);
        }

        producer.start();
        for (int i = 0; i < 5; i++)
            try {
                {
                    Message msg = new Message(topic,
                            "TagA",
                            String.valueOf(i),
                            String.valueOf(i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                    MessageQueue q = queues.get(i%queueLength);
                    SendResult sendResult = producer.send(msg, q);
                    System.out.printf("%s%n", sendResult);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        producer.shutdown();
    }
}
