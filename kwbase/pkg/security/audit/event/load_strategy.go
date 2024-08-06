// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package event

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/rules"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

type auditStrategyCache struct {
	syncutil.Mutex
	tableVersion uint32
	// auditStrategy is a mapping from Strategy to auditStrategy.
	auditStrategy map[string]rules.Strategy
}

var auditsStrategy auditStrategyCache

// AuditsOfWithOption looks up all the Strategy which is enabled.
func (ae *AuditEvent) AuditsOfWithOption(
	ctx context.Context, txn *kv.Txn,
) map[string]rules.Strategy {

	tableVersion := ae.GetTableVersion()
	// We loop in case the table version changes while we're looking up memberships.
	for {
		// Check version and maybe clear cache while holding the mutex.
		// We release the lock here instead of using defer as we need to keep
		// going and re-lock if adding the looked-up entry.
		auditsStrategy.Lock()
		if auditsStrategy.tableVersion != tableVersion {
			// Update version and drop the map.
			auditsStrategy.tableVersion = ae.GetTableVersion()
			auditsStrategy.auditStrategy = make(map[string]rules.Strategy)
		} else {
			strategy := auditsStrategy.auditStrategy
			auditsStrategy.Unlock()
			return strategy
		}
		auditsStrategy.Unlock()

		// Lookup strategy outside the lock from the table.
		strategyQuery, err := ae.auditQuery.QueryAuditPolicy(ctx, txn)
		if err != nil {
			return nil
		}
		// Update auditStrategy.
		auditsStrategy.Lock()
		if auditsStrategy.tableVersion != tableVersion {
			tableVersion = auditsStrategy.tableVersion
			auditsStrategy.Unlock()
			continue
		}
		// Table version remains the same.
		auditsStrategy.auditStrategy = strategyQuery
		auditsStrategy.Unlock()
		return strategyQuery
	}
}
