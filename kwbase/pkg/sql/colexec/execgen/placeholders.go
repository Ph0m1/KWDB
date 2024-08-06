// Copyright 2019 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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

package execgen

import "gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"

const nonTemplatePanic = "do not call from non-template code"

// Remove unused warnings.
var (
	_ = UNSAFEGET
	_ = COPYVAL
	_ = SET
	_ = SLICE
	_ = COPYSLICE
	_ = APPENDSLICE
	_ = APPENDVAL
	_ = LEN
	_ = ZERO
	_ = RANGE
	_ = WINDOW
)

// UNSAFEGET is a template function. Use this if you are not keeping data around
// (including passing it to SET).
func UNSAFEGET(target, i interface{}) interface{} {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
	return nil
}

// COPYVAL is a template function that can be used to set a scalar to the value
// of another scalar in such a way that the destination won't be modified if the
// source is. You must use this on the result of UNSAFEGET if you wish to store
// that result past the lifetime of the batch you UNSAFEGET'd from.
func COPYVAL(dest, src interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// SET is a template function.
func SET(target, i, new interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// SLICE is a template function.
func SLICE(target, start, end interface{}) interface{} {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
	return nil
}

// COPYSLICE is a template function.
func COPYSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// APPENDSLICE is a template function.
func APPENDSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// APPENDVAL is a template function.
func APPENDVAL(target, v interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// LEN is a template function.
func LEN(target interface{}) interface{} {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
	return nil
}

// ZERO is a template function.
func ZERO(target interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// RANGE is a template function.
func RANGE(loopVariableIdent, target, start, end interface{}) bool {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
	return false
}

// WINDOW is a template function.
func WINDOW(target, start, end interface{}) interface{} {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
	return nil
}
