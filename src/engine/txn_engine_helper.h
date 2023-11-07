// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGODB_TXN_ENGINE_HELPER_H_
#define DINGODB_TXN_ENGINE_HELPER_H_

#include <sys/stat.h>

#include <memory>
#include <vector>

#include "butil/status.h"
#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "meta/store_meta_manager.h"
#include "proto/store.pb.h"

namespace dingodb {

class TxnEngineHelper {
 public:
  static bool CheckLockConflict(const pb::store::LockInfo &lock_info, pb::store::IsolationLevel isolation_level,
                                int64_t start_ts, pb::store::TxnResultInfo &txn_result_info);
  static butil::Status GetLockInfo(RawEngine::ReaderPtr reader, const std::string &key, pb::store::LockInfo &lock_info);

  static butil::Status ScanLockInfo(RawEnginePtr raw_engine, int64_t min_lock_ts, int64_t max_lock_ts,
                                    const std::string &start_key, const std::string &end_key, uint32_t limit,
                                    std::vector<pb::store::LockInfo> &lock_infos);

  static butil::Status BatchGet(RawEnginePtr raw_engine, const pb::store::IsolationLevel &isolation_level,
                                int64_t start_ts, const std::vector<std::string> &keys,
                                std::vector<pb::common::KeyValue> &kvs, pb::store::TxnResultInfo &txn_result_info);

  static butil::Status ScanGetNextKeyValue(RawEngine::ReaderPtr reader, std::shared_ptr<Iterator> write_iter,
                                           std::shared_ptr<Iterator> lock_iter, int64_t start_ts,
                                           const pb::store::IsolationLevel &isolation_level,
                                           const std::string &start_iter_key, std::string &last_lock_key,
                                           std::string &last_write_key, pb::store::TxnResultInfo &txn_result_info,
                                           std::string &iter_key, std::string &data_value);

  static butil::Status Scan(RawEnginePtr raw_engine, const pb::store::IsolationLevel &isolation_level, int64_t start_ts,
                            const pb::common::Range &range, int64_t limit, bool key_only, bool is_reverse,
                            pb::store::TxnResultInfo &txn_result_info, std::vector<pb::common::KeyValue> &kvs,
                            bool &has_more, std::string &end_key);

  static butil::Status GetWriteInfo(RawEnginePtr raw_engine, int64_t min_commit_ts, int64_t max_commit_ts,
                                    int64_t start_ts, const std::string &key, bool include_rollback,
                                    bool include_delete, bool include_put, pb::store::WriteInfo &write_info,
                                    int64_t &commit_ts);

  static butil::Status GetRollbackInfo(RawEngine::ReaderPtr write_reader, int64_t start_ts, const std::string &key,
                                       pb::store::WriteInfo &write_info);

  // txn write functions
  static butil::Status DoTxnCommit(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                   std::shared_ptr<Context> ctx, store::RegionPtr region,
                                   const std::vector<pb::store::LockInfo> &lock_infos, int64_t start_ts,
                                   int64_t commit_ts);

  static butil::Status DoRollback(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                  std::shared_ptr<Context> ctx, std::vector<std::string> &keys_to_rollback_with_data,
                                  std::vector<std::string> &keys_to_rollback_without_data, int64_t start_ts);

  static butil::Status PessimisticLock(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                       std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation> &mutations,
                                       const std::string &primary_lock, int64_t start_ts, int64_t lock_ttl,
                                       int64_t for_update_ts);

  static butil::Status PessimisticRollback(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                           std::shared_ptr<Context> ctx, int64_t start_ts, int64_t for_update_ts,
                                           const std::vector<std::string> &keys);

  static butil::Status Prewrite(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                std::shared_ptr<Context> ctx, const std::vector<pb::store::Mutation> &mutations,
                                const std::string &primary_lock, int64_t start_ts, int64_t lock_ttl, int64_t txn_size,
                                bool try_one_pc, int64_t max_commit_ts, const std::vector<int32_t> &pessimistic_checks,
                                const std::map<int32_t, int64_t> &for_update_ts_checks,
                                const std::map<int32_t, std::string> &lock_extra_datas);

  static butil::Status Commit(RawEnginePtr raw_engine, std::shared_ptr<Engine> engine, std::shared_ptr<Context> ctx,
                              int64_t start_ts, int64_t commit_ts, const std::vector<std::string> &keys);

  static butil::Status BatchRollback(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                     std::shared_ptr<Context> ctx, int64_t start_ts,
                                     const std::vector<std::string> &keys);

  static butil::Status CheckTxnStatus(RawEnginePtr raw_engine, std::shared_ptr<Engine> engine,
                                      std::shared_ptr<Context> ctx, const std::string &primary_key, int64_t lock_ts,
                                      int64_t caller_start_ts, int64_t current_ts);

  static butil::Status ResolveLock(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                   std::shared_ptr<Context> ctx, int64_t start_ts, int64_t commit_ts,
                                   const std::vector<std::string> &keys);

  static butil::Status HeartBeat(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                 std::shared_ptr<Context> ctx, const std::string &primary_lock, int64_t start_ts,
                                 int64_t advise_lock_ttl);

  static butil::Status DeleteRange(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine,
                                   std::shared_ptr<Context> ctx, const std::string &start_key,
                                   const std::string &end_key);

  static butil::Status Gc(RawEnginePtr raw_engine, std::shared_ptr<Engine> raft_engine, std::shared_ptr<Context> ctx,
                          int64_t safe_point_ts);
};

}  // namespace dingodb

#endif  // DINGODB_TXN_ENGINE_HELPER_H_