// Copyright 2018 The Cockroach Authors.
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

package ledger

import (
	"encoding/binary"
	"hash"
	"math"
	"math/rand"
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/uint128"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

const (
	numTxnsPerCustomer    = 2
	numEntriesPerTxn      = 4
	numEntriesPerCustomer = numTxnsPerCustomer * numEntriesPerTxn

	paymentIDPrefix  = "payment:"
	txnTypeReference = 400
	cashMoneyType    = "C"
)

var ledgerCustomerColTypes = []coltypes.T{
	coltypes.Int64,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Bool,
	coltypes.Bool,
	coltypes.Bytes,
	coltypes.Int64,
	coltypes.Int64,
	coltypes.Int64,
}

func (w *ledger) ledgerCustomerInitialRow(rowIdx int) []interface{} {
	rng := w.rngPool.Get().(*rand.Rand)
	defer w.rngPool.Put(rng)
	rng.Seed(w.seed + int64(rowIdx))

	return []interface{}{
		rowIdx,                // id
		strconv.Itoa(rowIdx),  // identifier
		nil,                   // name
		randCurrencyCode(rng), // currency_code
		true,                  // is_system_customer
		true,                  // is_active
		randTimestamp(rng),    // created
		0,                     // balance
		nil,                   // credit_limit
		-1,                    // sequence
	}
}

func (w *ledger) ledgerCustomerSplitRow(splitIdx int) []interface{} {
	return []interface{}{
		(splitIdx + 1) * (w.customers / w.splits),
	}
}

var ledgerTransactionColTypes = []coltypes.T{
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Int64,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Bytes,
}

func (w *ledger) ledgerTransactionInitialRow(rowIdx int) []interface{} {
	rng := w.rngPool.Get().(*rand.Rand)
	defer w.rngPool.Put(rng)
	rng.Seed(w.seed + int64(rowIdx))

	h := w.hashPool.Get().(hash.Hash64)
	defer w.hashPool.Put(h)
	defer h.Reset()

	return []interface{}{
		w.ledgerStablePaymentID(rowIdx), // external_id
		nil,                             // tcomment
		randContext(rng),                // context
		txnTypeReference,                // transaction_type_reference
		randUsername(rng),               // username
		randTimestamp(rng),              // created_ts
		randTimestamp(rng),              // systimestamp
		nil,                             // reversed_by
		randResponse(rng),               // response
	}
}

func (w *ledger) ledgerTransactionSplitRow(splitIdx int) []interface{} {
	rng := rand.New(rand.NewSource(w.seed + int64(splitIdx)))
	u := uuid.FromUint128(uint128.FromInts(rng.Uint64(), rng.Uint64()))
	return []interface{}{
		paymentIDPrefix + u.String(),
	}
}

func (w *ledger) ledgerEntryInitialRow(rowIdx int) []interface{} {
	rng := w.rngPool.Get().(*rand.Rand)
	defer w.rngPool.Put(rng)
	rng.Seed(w.seed + int64(rowIdx))

	// Alternate.
	debit := rowIdx%2 == 0

	var amount float64
	if debit {
		amount = -float64(rowIdx) / 100
	} else {
		amount = float64(rowIdx-1) / 100
	}

	systemAmount := 88.122259
	if debit {
		systemAmount *= -1
	}

	cRowIdx := rowIdx / numEntriesPerCustomer

	tRowIdx := rowIdx / numEntriesPerTxn
	tID := w.ledgerStablePaymentID(tRowIdx)

	return []interface{}{
		rng.Int(),          // id
		amount,             // amount
		cRowIdx,            // customer_id
		tID,                // transaction_id
		systemAmount,       // system_amount
		randTimestamp(rng), // created_ts
		cashMoneyType,      // money_type
	}
}

func (w *ledger) ledgerEntrySplitRow(splitIdx int) []interface{} {
	return []interface{}{
		(splitIdx + 1) * (int(math.MaxInt64) / w.splits),
	}
}

func (w *ledger) ledgerSessionInitialRow(rowIdx int) []interface{} {
	rng := w.rngPool.Get().(*rand.Rand)
	defer w.rngPool.Put(rng)
	rng.Seed(w.seed + int64(rowIdx))

	return []interface{}{
		randSessionID(rng),   // session_id
		randTimestamp(rng),   // expiry_timestamp
		randSessionData(rng), // data
		randTimestamp(rng),   // last_update
	}
}

func (w *ledger) ledgerSessionSplitRow(splitIdx int) []interface{} {
	rng := rand.New(rand.NewSource(w.seed + int64(splitIdx)))
	return []interface{}{
		randSessionID(rng),
	}
}

func (w *ledger) ledgerStablePaymentID(tRowIdx int) string {
	h := w.hashPool.Get().(hash.Hash64)
	defer w.hashPool.Put(h)
	defer h.Reset()

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(tRowIdx))
	if _, err := h.Write(b); err != nil {
		panic(err)
	}
	hi := h.Sum64()
	if _, err := h.Write(b); err != nil {
		panic(err)
	}
	low := h.Sum64()

	u := uuid.FromUint128(uint128.FromInts(hi, low))
	return paymentIDPrefix + u.String()
}
