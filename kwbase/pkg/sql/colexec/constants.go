// Copyright 2019 The Cockroach Authors.
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
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package colexec

// DefaultVectorizeRowCountThreshold denotes the default row count threshold.
// When it is met, the vectorized execution engine will be used if possible.
// The current number 1000 was chosen upon comparing `SELECT count(*) FROM t`
// query running through the row and the vectorized execution engines on a
// single node with tables having different number of columns.
// Note: if you are updating this field, please make sure to update
// vectorize_threshold logic test accordingly.
const DefaultVectorizeRowCountThreshold = 1000

// VecMaxOpenFDsLimit specifies the maximum number of open file descriptors
// that the vectorized engine can have (globally) for use of the temporary
// storage.
const VecMaxOpenFDsLimit = 256

const defaultMemoryLimit = 64 << 20 /* 64 MiB */
