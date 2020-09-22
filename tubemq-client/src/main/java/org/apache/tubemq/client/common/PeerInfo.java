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

package org.apache.tubemq.client.common;


import org.apache.tubemq.corebase.TBaseConstants;
import org.apache.tubemq.corebase.cluster.Partition;


public class PeerInfo {
    private int partitionId = TBaseConstants.META_VALUE_UNDEFINED;
    private String brokerHost = "";
    private String partitionKey = "";
    private long currOffset = TBaseConstants.META_VALUE_UNDEFINED;
    private long maxOffset = TBaseConstants.META_VALUE_UNDEFINED;

    public PeerInfo() {

    }

    public PeerInfo(Partition partition, long newOffset, long maxOffset) {
        setMsgSourceInfo(partition, newOffset, maxOffset);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public long getCurrOffset() {
        return currOffset;
    }

    public void setMsgSourceInfo(Partition partition, long newOffset, long maxOffset) {
        if (partition != null) {
            partitionId = partition.getPartitionId();
            brokerHost = partition.getHost();
            partitionKey = partition.getPartitionKey();
        }
        this.currOffset = newOffset;
        this.maxOffset = maxOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }
}
