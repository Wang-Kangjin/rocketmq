package org.apache.rocketmq.example.mergeread;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * Created by wkj on 2018/8/21.
 */
public class TestConsumer {
    public static void main(String[] args) throws Exception{
        OrderedConsumer oc = new OrderedConsumer("MergeSort", "Merge", "TagA","localhost:9876");
        oc.start();
        while(!oc.isEmpty()){
            MessageExt msg = oc.receive();
            String msgBody = new String(msg.getBody());
            long ts = msg.getBornTimestamp();
            System.out.printf("ts:%d %s\n", ts, msgBody);
        }
    }
}
