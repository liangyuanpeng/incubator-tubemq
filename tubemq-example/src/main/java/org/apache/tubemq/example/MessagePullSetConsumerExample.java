/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.example;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.tubemq.client.config.ConsumerConfig;
import org.apache.tubemq.client.consumer.ConsumeOffsetInfo;
import org.apache.tubemq.client.consumer.ConsumerResult;
import org.apache.tubemq.client.consumer.PullMessageConsumer;
import org.apache.tubemq.client.exception.TubeClientException;
import org.apache.tubemq.client.factory.MessageSessionFactory;
import org.apache.tubemq.client.factory.TubeSingleSessionFactory;
import org.apache.tubemq.corebase.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This demo shows how to reset offset on consuming. The main difference from {@link MessagePullConsumerExample}
 * is that we call {@link PullMessageConsumer#completeSubscribe(String, int, boolean, Map)} instead of
 * {@link PullMessageConsumer#completeSubscribe()}. The former supports multiple options to configure
 * when to reset offset.
 */
public final class MessagePullSetConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(MessagePullSetConsumerExample.class);
    private static final AtomicLong counter = new AtomicLong(0);

    private final PullMessageConsumer messagePullConsumer;
    private final MessageSessionFactory messageSessionFactory;

    public MessagePullSetConsumerExample(String masterHostAndPort, String group) throws Exception {
        ConsumerConfig consumerConfig = new ConsumerConfig(masterHostAndPort, group);
        this.messageSessionFactory = new TubeSingleSessionFactory(consumerConfig);
        this.messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
    }

    public static void main(String[] args) {
        final String masterHostAndPort = args[0];
        final String topics = args[1];
        final String group = args[2];
        final int consumeCount = Integer.parseInt(args[3]);
        final Map<String, Long> partOffsetMap = new ConcurrentHashMap<>();
        partOffsetMap.put("123:test_1:0", 0L);
        partOffsetMap.put("123:test_1:1", 0L);
        partOffsetMap.put("123:test_1:2", 0L);
        partOffsetMap.put("123:test_2:0", 350L);
        partOffsetMap.put("123:test_2:1", 350L);
        partOffsetMap.put("123:test_2:2", 350L);

        final List<String> topicList = Arrays.asList(topics.split(","));

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    int getCount = consumeCount;
                    MessagePullSetConsumerExample messageConsumer =
                        new MessagePullSetConsumerExample(masterHostAndPort, group);
                    messageConsumer.subscribe(topicList, partOffsetMap);
                    // main logic of consuming
                    do {
                        ConsumerResult result = messageConsumer.getMessage();
                        if (result.isSuccess()) {
                            List<Message> messageList = result.getMessageList();
                            if (messageList != null) {
                                logger.info("Receive messages:" + counter.addAndGet(messageList.size()));
                            }

                            // Offset returned by GetMessage represents the initial offset of this request
                            // if consumer group is pure Pull mode, the initial offset can be saved;
                            // if not, we have to use the return value of confirmConsume
                            long oldValue = partOffsetMap.get(
                                result.getPartitionKey()) == null
                                ? -1
                                : partOffsetMap.get(result.getPartitionKey());
                            partOffsetMap.put(result.getPartitionKey(), result.getCurrOffset());
                            logger.info(
                                "GetMessage , partitionKey={}, oldValue={}, newVal={}",
                                new Object[]{result.getPartitionKey(), oldValue, result.getCurrOffset()});

                            // save the Offset from the return value of confirmConsume
                            ConsumerResult confirmResult = messageConsumer.confirmConsume(
                                result.getConfirmContext(),
                                true);
                            if (confirmResult.isSuccess()) {
                                oldValue = partOffsetMap.get(
                                    result.getPartitionKey()) == null
                                    ? -1
                                    : partOffsetMap.get(result.getPartitionKey());
                                partOffsetMap.put(result.getPartitionKey(), confirmResult.getCurrOffset());
                                logger.info(
                                    "ConfirmConsume , partitionKey={}, oldValue={}, newVal={}",
                                    new Object[]{
                                        confirmResult.getPartitionKey(),
                                        oldValue,
                                        confirmResult.getCurrOffset()});
                            } else {
                                logger.info(
                                    "ConfirmConsume failure, errCode is {}, errInfo is {}.",
                                    confirmResult.getErrCode(),
                                    confirmResult.getErrMsg());
                            }
                        } else {
                            if (!(result.getErrCode() == 400
                                    || result.getErrCode() == 404
                                    || result.getErrCode() == 405
                                    || result.getErrCode() == 406
                                    || result.getErrCode() == 407
                                    || result.getErrCode() == 408)) {
                                logger.info(
                                        "Receive messages errorCode is {}, Error message is {}",
                                        result.getErrCode(),
                                        result.getErrMsg());
                            }
                        }
                        if (consumeCount >= 0) {
                            if (--getCount <= 0) {
                                break;
                            }
                        }
                    } while (true);
                } catch (Exception e) {
                    logger.error("Create consumer failed!", e);
                }
            }
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(60 * 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Thread Pool shutdown has been interrupted!");
        }
    }

    public void subscribe(
        List<String> topicList,
        Map<String, Long> partOffsetMap
    ) throws TubeClientException {
        TreeSet<String> filters = new TreeSet<>();
        filters.add("aaa");
        filters.add("bbb");
        for (String topic : topicList) {
            this.messagePullConsumer.subscribe(topic, filters);
        }
        String sessionKey = "test_reset2";
        int consumerCount = 2;
        boolean isSelectBig = false;
        messagePullConsumer.completeSubscribe(
            sessionKey,
            consumerCount,
            isSelectBig,
            partOffsetMap);
    }

    public ConsumerResult getMessage() throws TubeClientException {
        return messagePullConsumer.getMessage();
    }

    public ConsumerResult confirmConsume(
        String confirmContext,
        boolean isConsumed
    ) throws TubeClientException {
        return messagePullConsumer.confirmConsume(confirmContext, isConsumed);
    }

    public Map<String, ConsumeOffsetInfo> getCurrPartitionOffsetMap() throws TubeClientException {
        return messagePullConsumer.getCurConsumedPartitions();
    }
}


