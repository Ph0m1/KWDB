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

// Package ptprovider encapsulates the concrete implementation of the
// protectedts.Provider.
package ptprovider

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptcache"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptreconcile"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptstorage"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptverifier"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// Config configures the Provider.
type Config struct {
	Settings             *cluster.Settings
	DB                   *kv.DB
	Stores               *kvserver.Stores
	ReconcileStatusFuncs ptreconcile.StatusFuncs
	InternalExecutor     sqlutil.InternalExecutor
}

type provider struct {
	protectedts.Storage
	protectedts.Verifier
	protectedts.Cache
}

// New creates a new protectedts.Provider.
func New(cfg Config) (protectedts.Provider, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	storage := ptstorage.New(cfg.Settings, cfg.InternalExecutor)
	verifier := ptverifier.New(cfg.DB, storage)
	cache := ptcache.New(ptcache.Config{
		DB:       cfg.DB,
		Storage:  storage,
		Settings: cfg.Settings,
	})
	return &provider{
		Storage:  storage,
		Cache:    cache,
		Verifier: verifier,
	}, nil
}

func validateConfig(cfg Config) error {
	switch {
	case cfg.Settings == nil:
		return errors.Errorf("invalid nil Settings")
	case cfg.DB == nil:
		return errors.Errorf("invalid nil DB")
	case cfg.InternalExecutor == nil:
		return errors.Errorf("invalid nil InternalExecutor")
	default:
		return nil
	}
}

func (p *provider) Start(ctx context.Context, stopper *stop.Stopper) error {
	return p.Cache.(*ptcache.Cache).Start(ctx, stopper)
}
