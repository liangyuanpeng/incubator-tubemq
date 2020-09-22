/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef TUBEMQ_CLIENT_CONFIGURE_H_
#define TUBEMQ_CLIENT_CONFIGURE_H_

#include <stdint.h>
#include <stdio.h>

#include <map>
#include <set>
#include <string>

namespace tubemq {

using std::map;
using std::set;
using std::string;


class BaseConfig {
 public:
  BaseConfig();
  ~BaseConfig();
  BaseConfig& operator=(const BaseConfig& target);
  bool SetMasterAddrInfo(string& err_info, const string& master_addrinfo);
  bool SetTlsInfo(string& err_info, bool tls_enable, const string& trust_store_path,
                  const string& trust_store_password);
  bool SetAuthenticInfo(string& err_info, bool authentic_enable, const string& usr_name,
                        const string& usr_password);
  const string& GetMasterAddrInfo() const;
  bool IsTlsEnabled();
  const string& GetTrustStorePath() const;
  const string& GetTrustStorePassword() const;
  bool IsAuthenticEnabled();
  const string& GetUsrName() const;
  const string& GetUsrPassWord() const;
  // set the rpc timout, unit Millis-second, duration [8000, 300000],
  // default 15000 Millis-seconds;
  void SetRpcReadTimeoutMs(int32_t rpc_read_timeout_ms);
  int32_t GetRpcReadTimeoutMs();
  // Set the duration of the client's heartbeat cycle, in Millis-seconds,
  // the default is 10000 Millis-seconds
  void SetHeartbeatPeriodMs(int32_t heartbeat_period_ms);
  int32_t GetHeartbeatPeriodMs();
  void SetMaxHeartBeatRetryTimes(int32_t max_heartbeat_retry_times);
  int32_t GetMaxHeartBeatRetryTimes();
  void SetHeartbeatPeriodAftFailMs(int32_t heartbeat_period_afterfail_ms);
  int32_t GetHeartbeatPeriodAftFailMs();
  string ToString();

 private:
  string master_addrinfo_;
  // user authenticate
  bool auth_enable_;
  string auth_usrname_;
  string auth_usrpassword_;
  // TLS configuration
  bool tls_enabled_;
  string tls_trust_store_path_;
  string tls_trust_store_password_;
  // other setting
  int32_t rpc_read_timeout_ms_;
  int32_t heartbeat_period_ms_;
  int32_t max_heartbeat_retry_times_;
  int32_t heartbeat_period_afterfail_ms_;
};

enum ConsumePosition {
  kConsumeFromFirstOffset = -1,
  kConsumeFromLatestOffset = 0,
  kComsumeFromMaxOffsetAlways = 1
};  // enum ConsumePosition

class ConsumerConfig : public BaseConfig {
 public:
  ConsumerConfig();
  ~ConsumerConfig();
  ConsumerConfig& operator=(const ConsumerConfig& target);
  bool SetGroupConsumeTarget(string& err_info, const string& group_name,
                             const set<string>& subscribed_topicset);
  bool SetGroupConsumeTarget(string& err_info, const string& group_name,
                             const map<string, set<string> >& subscribed_topic_and_filter_map);
  bool SetGroupConsumeTarget(string& err_info, const string& group_name,
                             const map<string, set<string> >& subscribed_topic_and_filter_map,
                             const string& session_key, uint32_t source_count, bool is_select_big,
                             const map<string, int64_t>& part_offset_map);
  bool IsBoundConsume() const { return is_bound_consume_; }
  const string& GetSessionKey() const { return session_key_; }
  const uint32_t GetSourceCount() const { return source_count_; }
  bool IsSelectBig() const { return is_select_big_; }
  const map<string, int64_t>& GetPartOffsetInfo() const { return part_offset_map_; }
  const string& GetGroupName() const;
  const map<string, set<string> >& GetSubTopicAndFilterMap() const;
  void SetConsumePosition(ConsumePosition consume_from_where);
  const ConsumePosition GetConsumePosition() const;
  const int32_t GetMsgNotFoundWaitPeriodMs() const;
  const int32_t GetMaxPartCheckPeriodMs() const;
  const uint32_t GetPartCheckSliceMs() const;
  void SetMsgNotFoundWaitPeriodMs(int32_t msg_notfound_wait_period_ms);
  void SetMaxPartCheckPeriodMs(int32_t max_part_check_period_ms);
  void SetPartCheckSliceMs(uint32_t part_check_slice_ms);
  const int32_t GetMaxSubinfoReportIntvl() const;
  void SetMaxSubinfoReportIntvl(int32_t max_subinfo_report_intvl);
  bool IsRollbackIfConfirmTimeout();
  void setRollbackIfConfirmTimeout(bool is_rollback_if_confirm_timeout);
  const int32_t GetWaitPeriodIfConfirmWaitRebalanceMs() const;
  void SetWaitPeriodIfConfirmWaitRebalanceMs(int32_t reb_confirm_wait_period_ms);
  const int32_t GetMaxConfirmWaitPeriodMs() const;
  void SetMaxConfirmWaitPeriodMs(int32_t max_confirm_wait_period_ms);
  const int32_t GetShutdownRebWaitPeriodMs() const;
  void SetShutdownRebWaitPeriodMs(int32_t wait_period_when_shutdown_ms);
  string ToString();

 private:
  bool setGroupConsumeTarget(string& err_info, bool is_bound_consume, const string& group_name,
                             const map<string, set<string> >& subscribed_topic_and_filter_map,
                             const string& session_key, uint32_t source_count, bool is_select_big,
                             const map<string, int64_t>& part_offset_map);

 private:
  string group_name_;
  map<string, set<string> > sub_topic_and_filter_map_;
  bool is_bound_consume_;
  string session_key_;
  uint32_t source_count_;
  bool is_select_big_;
  map<string, int64_t> part_offset_map_;
  ConsumePosition consume_position_;
  int32_t max_subinfo_report_intvl_;
  int32_t max_part_check_period_ms_;
  uint32_t part_check_slice_ms_;
  int32_t msg_notfound_wait_period_ms_;
  bool is_rollback_if_confirm_timout_;
  int32_t reb_confirm_wait_period_ms_;
  int32_t max_confirm_wait_period_ms_;
  int32_t shutdown_reb_wait_period_ms_;
};

}  // namespace tubemq

#endif  // TUBEMQ_CLIENT_CONFIGURE_H_
