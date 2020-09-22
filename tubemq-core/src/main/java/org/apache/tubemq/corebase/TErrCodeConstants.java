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

package org.apache.tubemq.corebase;


public class TErrCodeConstants {
    public static final int SUCCESS = 200;
    public static final int NOT_READY = 201;
    public static final int MOVED = 301;

    public static final int BAD_REQUEST = 400;
    public static final int UNAUTHORIZED = 401;
    public static final int FORBIDDEN = 403;
    public static final int NOT_FOUND = 404;
    public static final int ALL_PARTITION_FROZEN = 405;
    public static final int NO_PARTITION_ASSIGNED = 406;
    public static final int ALL_PARTITION_WAITING = 407;
    public static final int ALL_PARTITION_INUSE = 408;
    public static final int PARTITION_OCCUPIED = 410;
    public static final int HB_NO_NODE = 411;
    public static final int DUPLICATE_PARTITION = 412;
    public static final int CERTIFICATE_FAILURE = 415;
    public static final int SERVER_RECEIVE_OVERFLOW = 419;
    public static final int CONSUME_GROUP_FORBIDDEN = 450;
    public static final int SERVER_CONSUME_SPEED_LIMIT = 452;
    public static final int CONSUME_CONTENT_FORBIDDEN = 455;

    public static final int INTERNAL_SERVER_ERROR = 500;
    public static final int SERVICE_UNAVAILABLE = 503;
    public static final int INTERNAL_SERVER_ERROR_MSGSET_NULL = 510;

}
