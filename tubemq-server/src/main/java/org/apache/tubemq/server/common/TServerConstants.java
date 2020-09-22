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

package org.apache.tubemq.server.common;

import org.apache.tubemq.corebase.TBaseConstants;


public final class TServerConstants {

    public static final String TOKEN_JOB_TOPICS = "topics";
    public static final String TOKEN_JOB_STORE_MGR = "messageStoreManager";
    public static final String TOKEN_DEFAULT_FLOW_CONTROL = "default_master_ctrl";
    public static final String TOKEN_BLANK_FILTER_CONDITION = ",,";

    public static final int CFG_MODAUTHTOKEN_MAX_LENGTH = 128;
    public static final int CFG_ROWLOCK_DEFAULT_DURATION = 30000;
    public static final int CFG_ZK_COMMIT_DEFAULT_RETRIES = 10;
    public static final int CFG_STORE_DEFAULT_MSG_READ_UNIT = 327680;
    public static final int CFG_BATCH_BROKER_OPERATE_MAX_COUNT = 50;
    public static final int CFG_BATCH_RECORD_OPERATE_MAX_COUNT = 100;

    public static final int CFG_DEFAULT_DATA_UNFLUSH_HOLD = 0;
    public static final int CFG_DEFAULT_CONSUME_RULE = 300;
    public static final int CFG_DELETEWHEN_MAX_LENGTH = 1024;
    public static final int CFG_DELETEPOLICY_MAX_LENGTH = 1024;
    public static final int CFG_CONSUMER_CLIENTID_MAX_LENGTH =
            TBaseConstants.META_MAX_GROUPNAME_LENGTH + 512;

    public static final long CFG_REPORT_DEFAULT_SYNC_DURATION = 2 * 3600 * 1000;
    public static final long CFG_STORE_STATS_MAX_REFRESH_DURATION = 20 * 60 * 1000;

}
