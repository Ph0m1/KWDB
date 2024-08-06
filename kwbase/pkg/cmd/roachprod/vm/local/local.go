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

package local

import (
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cmd/roachprod/config"
	"gitee.com/kwbasedb/kwbase/pkg/cmd/roachprod/install"
	"gitee.com/kwbasedb/kwbase/pkg/cmd/roachprod/vm"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// ProviderName is config.Local.
const ProviderName = config.Local

func init() {
	vm.Providers[ProviderName] = &Provider{}
}

// A Provider is used to create stub VM objects.
type Provider struct{}

// No-op implementation of ProviderFlags
type emptyFlags struct{}

// ConfigureCreateFlags is part of ProviderFlags.  This implementation is a no-op.
func (o *emptyFlags) ConfigureCreateFlags(flags *pflag.FlagSet) {
}

// ConfigureClusterFlags is part of ProviderFlags.  This implementation is a no-op.
func (o *emptyFlags) ConfigureClusterFlags(*pflag.FlagSet, vm.MultipleProjectsOption) {
}

// CleanSSH is part of the vm.Provider interface.  This implementation is a no-op.
func (p *Provider) CleanSSH() error {
	return nil
}

// ConfigSSH is part of the vm.Provider interface.  This implementation is a no-op.
func (p *Provider) ConfigSSH() error {
	return nil
}

// Create just creates fake host-info entries in the local filesystem
func (p *Provider) Create(names []string, opts vm.CreateOpts) error {
	path := filepath.Join(os.ExpandEnv(config.DefaultHostDir), config.Local)
	file, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "problem creating file %s", path)
	}
	defer file.Close()

	// Align columns left and separate with at least two spaces.
	tw := tabwriter.NewWriter(file, 0, 8, 2, ' ', 0)
	if _, err := tw.Write([]byte("# user@host\tlocality\n")); err != nil {
		return err
	}
	for i := 0; i < len(names); i++ {
		if _, err := tw.Write([]byte(fmt.Sprintf(
			"%s@%s\t%s\n", config.OSUser.Username, "127.0.0.1", "region=local,zone=local"))); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return errors.Wrapf(err, "problem writing file %s", path)
	}
	return nil
}

// Delete is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) Delete(vms vm.List) error {
	return nil
}

// Extend is part of the vm.Provider interface.  This implementation returns an error.
func (p *Provider) Extend(vms vm.List, lifetime time.Duration) error {
	return errors.New("local clusters have unlimited lifetime")
}

// FindActiveAccount is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) FindActiveAccount() (string, error) {
	return "", nil
}

// Flags is part of the vm.Provider interface. This implementation is a no-op.
func (p *Provider) Flags() vm.ProviderFlags {
	return &emptyFlags{}
}

// List constructs N-many localhost VM instances, using SyncedCluster as a way to remember
// how many nodes we should have
func (p *Provider) List() (ret vm.List, _ error) {
	if sc, ok := install.Clusters[ProviderName]; ok {
		now := timeutil.Now()
		for range sc.VMs {
			ret = append(ret, vm.VM{
				Name:        "localhost",
				CreatedAt:   now,
				Lifetime:    time.Hour,
				PrivateIP:   "127.0.0.1",
				Provider:    ProviderName,
				ProviderID:  ProviderName,
				PublicIP:    "127.0.0.1",
				RemoteUser:  config.OSUser.Username,
				VPC:         ProviderName,
				MachineType: ProviderName,
				Zone:        ProviderName,
			})
		}
	}
	return
}

// Name returns the name of the Provider, which will also surface in VM.Provider
func (p *Provider) Name() string {
	return ProviderName
}

// Active is part of the vm.Provider interface.
func (p *Provider) Active() bool {
	return true
}
