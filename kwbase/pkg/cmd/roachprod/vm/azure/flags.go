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

package azure

import (
	"fmt"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cmd/roachprod/vm"
	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2019-07-01/compute"
	"github.com/spf13/pflag"
)

type providerOpts struct {
	locations        []string
	machineType      string
	operationTimeout time.Duration
	syncDelete       bool
	vnetName         string
	zone             string
	networkDiskType  string
}

var defaultLocations = []string{
	"eastus2",
	"westus",
	"westeurope",
}

var defaultZone = "1"

// ConfigureCreateFlags implements vm.ProviderFlags.
func (o *providerOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
	flags.DurationVar(&o.operationTimeout, ProviderName+"-timeout", 10*time.Minute,
		"The maximum amount of time for an Azure API operation to take")
	flags.BoolVar(&o.syncDelete, ProviderName+"-sync-delete", false,
		"Wait for deletions to finish before returning")
	flags.StringVar(&o.machineType, ProviderName+"-machine-type",
		string(compute.VirtualMachineSizeTypesStandardD4V3),
		"Machine type (see https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)")
	flags.StringSliceVar(&o.locations, ProviderName+"-locations", nil,
		fmt.Sprintf("Locations for cluster (see `az account list-locations`) (default\n[%s])",
			strings.Join(defaultLocations, ",")))
	flags.StringVar(&o.vnetName, ProviderName+"-vnet-name", "common",
		"The name of the VNet to use")
	flags.StringVar(&o.zone, ProviderName+"-availability-zone", "", "Availability Zone to create VMs in")
	flags.StringVar(&o.networkDiskType, ProviderName+"-network-disk-type", "premium-disk",
		"type of network disk [premium-disk, ultra-disk]. only used if local-ssd is false")
}

// ConfigureClusterFlags implements vm.ProviderFlags and is a no-op.
func (o *providerOpts) ConfigureClusterFlags(*pflag.FlagSet, vm.MultipleProjectsOption) {
}
