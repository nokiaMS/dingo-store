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

#ifndef DINGODB_SDK_SUPERVISOR_H_
#define DINGODB_SDK_SUPERVISOR_H_

#include "sdk/coordinator_proxy.h"

namespace dingodb {
namespace sdk {

class Supervisor {
 public:
  Supervisor(const Supervisor&) = delete;
  const Supervisor& operator=(const Supervisor&) = delete;

  explicit Supervisor(std::shared_ptr<CoordinatorProxy> coordinator_proxy);

  virtual ~Supervisor() = default;

  Status IsCreateRegionInProgress(int64_t region_id, bool& out_create_in_progress);

  Status DropRegion(int64_t region_id);

 private:
  std::shared_ptr<CoordinatorProxy> coordinator_proxy_;
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_SUPERVISOR_H_