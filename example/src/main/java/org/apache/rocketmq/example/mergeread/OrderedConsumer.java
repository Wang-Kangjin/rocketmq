package org.apache.rocketmq.example.mergeread;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.Set;

/**
 * Created by wkj on 2018/8/21.
 */
public class OrderedConsumer {
    DefaultMQPullConsumer consumer;
    Set<MessageQueue> mqs;
    String topic;
    String tag;
    String groupName;
    ArrayList<HeapNode> heap;

    public OrderedConsumer(String groupName, String topic, String tag, String namesvrAddr){
        this.consumer = new DefaultMQPullConsumer(groupName);
        consumer.setNamesrvAddr(namesvrAddr);
        this.topic = topic;
        this.groupName = groupName;
        this.tag = tag;
        heap = new ArrayList<>();
    }

    public void start() throws MQClientException,MQBrokerException, InterruptedException,RemotingException {
        this.consumer.start();
        mqs = consumer.fetchSubscribeMessageQueues(topic);
        for (MessageQueue mq : mqs) {
            PullResult pullResult = consumer.pullBlockIfNotFound(mq, tag, 0L, 1);
            HeapNode heapNode = new HeapNode(pullResult, mq);
            System.out.println(heapNode.timestamp);
            heap.add(heapNode);
        }
        System.out.println(heap.size());
        for(int i = (mqs.size()-1)/2; i>=0; i--) {
            adjustHeap(heap, i);
        }
        adjustHeap(heap, 0);
    }

    public boolean isEmpty(){
        return heapIsEmpty(heap);
    }

    private  void adjustHeap(ArrayList<HeapNode> heapTree, int startAt){
        int leftChildIndex = startAt*2+1;
        int rightChildIndex = startAt*2+2;
        int smallerChildIndex = -1;

        HeapNode parent = heapTree.get(startAt);
        if (leftChildIndex > heapTree.size()-1)
            return;
        if (rightChildIndex > heapTree.size()-1)
            smallerChildIndex = leftChildIndex;
        else{

            HeapNode leftChild = heapTree.get(leftChildIndex);
            HeapNode rightChild = heapTree.get(rightChildIndex);
            if ( leftChild.timestamp < rightChild.timestamp ){
                smallerChildIndex = leftChildIndex;
            }else{
                smallerChildIndex = rightChildIndex;
            }
        }

        HeapNode smallerHeapNode = heapTree.get(smallerChildIndex);
        if (parent.timestamp > smallerHeapNode.timestamp || (parent.isNull && !smallerHeapNode.isNull)) {
            //exchange parent and child
            heapTree.set(startAt, smallerHeapNode);
            heapTree.set(smallerChildIndex, parent);
            adjustHeap(heapTree, smallerChildIndex);
        }
    }

    public MessageExt receive() throws MQClientException,RemotingException,MQBrokerException,InterruptedException, Exception{
        if (heapIsEmpty(heap)) {
            throw new Exception("all of queues is empty");
        }
        //printHeap(heap);
        //Step 1: fetch first from heap
        HeapNode topNode = heap.get(0);

        // consume

        String body = new String(topNode.pullResult.getMsgFoundList().get(0).getBody());
        MessageExt res  = topNode.pullResult.getMsgFoundList().get(0);

        //printHeap(heap);
        //Step 2: get Next
        //System.out.printf("get offset:%d\n", topNode.pullResult.getNextBeginOffset());
        PullResult pr = consumer.pull(topNode.q, tag, topNode.pullResult.getNextBeginOffset(), 1);
        topNode.setPullResult(pr);

        //Step 3: adjust Heap
        adjustHeap(heap,0);
        return res;
    }

    public void shutdown(){
        consumer.shutdown();
    }
    private  boolean heapIsEmpty(ArrayList<HeapNode> arr) {
        for(int i = 0; i<arr.size(); i++){
            HeapNode hn = arr.get(i);
            if (!hn.isNull) {
                return false;
            }
        }
        return true;
    }
}
class HeapNode {
    PullResult pullResult;
    MessageQueue q;
    boolean isNull;
    long timestamp;
    public HeapNode(PullResult pr, MessageQueue q){
        this.pullResult = pr;
        this.timestamp = pr.getMsgFoundList().get(0).getBornTimestamp();
        this.q = q;
    }
    public void setPullResult(PullResult pullResult) {
        switch (pullResult.getPullStatus()) {
            case FOUND:
                this.pullResult = pullResult;
                this.timestamp = pullResult.getMsgFoundList().get(0).getBornTimestamp();
                this.isNull = false;
                break;
            case NO_MATCHED_MSG:
                break;
            case NO_NEW_MSG:
                this.isNull = true;
                this.timestamp = Long.MAX_VALUE;
                break ;
            case OFFSET_ILLEGAL:
                break;
            default:
                break;
        }

    }
}

class QueueEmptyException extends Exception {
    private final String errorMessage;
    QueueEmptyException(String s){
        errorMessage = s;
    }
}