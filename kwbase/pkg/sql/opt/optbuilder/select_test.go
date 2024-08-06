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

package optbuilder

//func TestBuildTimeSeriesStmt(t *testing.T) {
//	defer func() {
//		if r := recover(); r != nil {
//			// This code allows us to propagate errors without adding lots of checks
//			// for `if err != nil` throughout the construction code. This is only
//			// possible because the code does not update shared state and does not
//			// manipulate locks.
//			if ok, e := errorutil.ShouldCatch(r); ok {
//				if e != nil && !strings.Contains(e.Error(), "Cross group queries are not allowed temporarily") {
//					t.Fatalf("%s", e)
//				}
//			} else {
//				panic(r)
//			}
//		}
//	}()
//	defer leaktest.AfterTest(t)()
//
//	testCases := []struct {
//		in      string
//		inscope *scope
//	}{
//		{"select * unionfrom t1.p1.d1", &scope{}},
//		{"select e1.* unionfrom t1.p1.d1", &scope{}},
//		{"select e1.value+e2.value unionfrom t1.p1.d1", &scope{}},
//		{"select * unionfrom t1.*.d1 satisfying device.color='red' and portal.size='10' or 'blue'=device.color and portal.size='20' or device.color='green'", &scope{}},
//		{"select e1.value,e3.value unionfrom t1.p1.d1", &scope{}},
//		{"select * unionfrom t1.p1.d1", &scope{}},
//		{"explain select time_bucket(e1.timestamp, '100s') as bucket, count(e1.value) unionfrom t1.p1.d1 group by bucket", &scope{}},
//		{"explain select time_bucket(e1.timestamp, '3600s') as bucket, count(e1.value) unionfrom t1.p1.d1 group by bucket", &scope{}},
//		{"explain select time_bucket(e1.timestamp, '3600s') as bucket, count(e1.value), count(e1.value)*sum(e2.value) unionfrom t1.p1.d1 group by bucket", &scope{}},
//		{"explain select time_bucket(e1.timestamp, '3600s') as bucket, count(e1.value), count(e1.value)*sum(e2.value), max(e1.value) unionfrom t1.p1.d1 group by bucket", &scope{}},
//	}
//	for i, tc := range testCases {
//		stmt, err := parser.ParseOne(fmt.Sprintf("%s", tc.in))
//		if err != nil {
//			t.Fatalf("%s can not parse", tc.in)
//		}
//		b := New(context.Background(), &tree.SemaContext{}, &tree.EvalContext{}, nil, &norm.Factory{}, stmt.AST)
//		b.evalCtx.SessionData = &sessiondata.SessionData{}
//		b.factory.Init(b.evalCtx, nil)
//		sel := stmt.AST.(*tree.Select)
//		s := sel.Select.(*tree.SelectClause)
//		if i == 3 {
//			tc.inscope = newScope()
//			_, tc.inscope.deviceList = tc.inscope.filterDeviceByExpr(s.From.Satisfy, &tc.inscope.deviceList)
//			if len(tc.inscope.deviceList) != 4 {
//				t.Fatalf("expected device count:%v, got: %v ", 4, len(tc.inscope.deviceList))
//			}
//			continue
//		}
//		if i < 3 {
//			tc.inscope = consturctScope(b)
//		} else {
//			tc.inscope = newEndpointGroupScope(b)
//		}
//		tc.inscope.builder = b
//		tc.inscope.physType = tree.TS
//		b.buildTimeSeriesStmt(s, nil, &scope{}, tc.inscope)
//	}
//}
//
//func consturctScope(b *Builder) *scope {
//	outScope := &scope{}
//	device := sqlbase.DeviceDesc{
//		DeviceName: "d1",
//		DeviceID:   1,
//		PortalID:   2,
//		Endpoints: map[uint64]sqlbase.EndpointGroup{
//			1: {
//				{
//					Name:        "e1",
//					EID:         1,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {2, *types.Int},
//						"valid":     {3, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//				{
//					Name:        "e2",
//					EID:         2,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {4, *types.Int},
//						"valid":     {5, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//			},
//		},
//		ZCols: []sqlbase.KWDBKColumn{
//			{
//				KObjectTableId: 2,
//				EndpointId:     0,
//				KColumnId:      1,
//				Name:           "timestamp",
//				SqlType:        *types.Timestamp,
//			},
//			{
//				KObjectTableId: 2,
//				EndpointId:     1,
//				KColumnId:      2,
//				Name:           "value",
//				SqlType:        *types.Int,
//			},
//			{
//				KObjectTableId: 2,
//				EndpointId:     1,
//				KColumnId:      2,
//				Name:           "valid",
//				SqlType:        *types.Bool,
//			},
//			{
//				KObjectTableId: 2,
//				EndpointId:     2,
//				KColumnId:      4,
//				Name:           "value",
//				SqlType:        *types.Int,
//			},
//			{
//				KObjectTableId: 2,
//				EndpointId:     2,
//				KColumnId:      5,
//				Name:           "valid",
//				SqlType:        *types.Bool,
//			},
//		},
//	}
//	tableName := tree.MakeDeviceName("t1", "p1", "d1")
//	tabMeta := b.addTable(&device, &tableName)
//	endpCount := len(device.Endpoints[1])
//	var endpIDs opt.ColSet
//
//	es := device.Endpoints[1]
//	for i := 0; i < endpCount; i++ {
//		endpoint := es[i]
//		colID := opt.ColumnID(endpoint.EID)
//		for _, col := range endpoint.ZColMap {
//			endpIDs.Add(opt.ColumnID(col.ID))
//		}
//		name := tree.Name(endpoint.Name)
//		// build scopecolumn based on endpoint information and store it in scope.cols
//		outScope.cols = append(outScope.cols, scopeColumn{
//			id:         colID,
//			name:       name,
//			table:      tableName,
//			typ:        &endpoint.Datatype,
//			kColMap:    endpoint.ZColMap,
//			kColsName:  endpoint.ZCols,
//			attributes: endpoint.Attributes,
//		})
//	}
//	outScope.deviceList = []sqlbase.DeviceInfo{
//		{
//			SearchPath: [2]string{"t1", "p1"},
//			Device:     &device,
//		},
//	}
//	private := memo.ScanPrivate{Table: tabMeta.MetaID, Cols: endpIDs}
//	outScope.expr = b.factory.ConstructScan(&private)
//	return outScope
//}
//
//func newScope() *scope {
//	outScope := &scope{}
//	device := sqlbase.DeviceDesc{
//		DeviceName: "d1",
//		Endpoints: map[uint64]sqlbase.EndpointGroup{
//			1: {
//				{
//					Name:        "e1",
//					EID:         1,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {2, *types.Int},
//						"valid":     {3, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//				{
//					Name:        "e2",
//					EID:         2,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {4, *types.Int},
//						"valid":     {5, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//			},
//		},
//		Attributes:       map[string]string{},
//		PortalAttributes: map[string]string{},
//	}
//	device.Attributes["color"] = "red"
//	device.PortalAttributes["size"] = "10"
//	outScope.deviceList = append(outScope.deviceList, sqlbase.DeviceInfo{
//		SearchPath: [2]string{"t1", "p1"},
//		Device:     &device,
//	},
//	)
//
//	device1 := sqlbase.DeviceDesc{
//		DeviceName: "d1",
//		Endpoints: map[uint64]sqlbase.EndpointGroup{
//			1: {
//				{
//					Name:        "e1",
//					EID:         1,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {2, *types.Int},
//						"valid":     {3, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//				{
//					Name:        "e2",
//					EID:         2,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {4, *types.Int},
//						"valid":     {5, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//			},
//		},
//		Attributes:       map[string]string{},
//		PortalAttributes: map[string]string{},
//	}
//	device1.Attributes["color"] = "blue"
//	device1.PortalAttributes["size"] = "20"
//	outScope.deviceList = append(outScope.deviceList, sqlbase.DeviceInfo{
//		SearchPath: [2]string{"t1", "p2"},
//		Device:     &device1,
//	},
//	)
//
//	device2 := sqlbase.DeviceDesc{
//		DeviceName: "d1",
//		Endpoints: map[uint64]sqlbase.EndpointGroup{
//			1: {
//				{
//					Name:        "e1",
//					EID:         1,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {2, *types.Int},
//						"valid":     {3, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//				{
//					Name:        "e2",
//					EID:         2,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {4, *types.Int},
//						"valid":     {5, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//			},
//		},
//		Attributes:       map[string]string{},
//		PortalAttributes: map[string]string{},
//	}
//	device2.Attributes["color"] = "green"
//	device2.PortalAttributes["size"] = "30"
//	outScope.deviceList = append(outScope.deviceList, sqlbase.DeviceInfo{
//		SearchPath: [2]string{"t1", "p3"},
//		Device:     &device2,
//	},
//	)
//
//	device3 := sqlbase.DeviceDesc{
//		DeviceName: "d1",
//		Endpoints: map[uint64]sqlbase.EndpointGroup{
//			1: {
//				{
//					Name:        "e1",
//					EID:         1,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {2, *types.Int},
//						"valid":     {3, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//				{
//					Name:        "e2",
//					EID:         2,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {4, *types.Int},
//						"valid":     {5, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//			},
//		},
//		Attributes:       map[string]string{},
//		PortalAttributes: map[string]string{},
//	}
//	device3.Attributes["color"] = "green"
//	device3.PortalAttributes["size"] = "40"
//	outScope.deviceList = append(outScope.deviceList, sqlbase.DeviceInfo{
//		SearchPath: [2]string{"t1", "p4"},
//		Device:     &device3,
//	},
//	)
//
//	return outScope
//}
//
//func newEndpointGroupScope(b *Builder) *scope {
//	outScope := &scope{}
//	device := sqlbase.DeviceDesc{
//		DeviceName: "d1",
//		Endpoints: map[uint64]sqlbase.EndpointGroup{
//			1: {
//				{
//					Name:        "e1",
//					EID:         1,
//					KObjTableID: 1,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {2, *types.Int},
//						"valid":     {3, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//				{
//					Name:        "e2",
//					EID:         2,
//					KObjTableID: 1,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {4, *types.Int},
//						"valid":     {5, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//			},
//			2: {
//				{
//					Name:        "e3",
//					EID:         3,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {2, *types.Int},
//						"valid":     {3, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//				{
//					Name:        "e4",
//					EID:         4,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {4, *types.Int},
//						"valid":     {5, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//			},
//		},
//		ZCols: []sqlbase.KWDBKColumn{
//			{
//				KObjectTableId: 1,
//				EndpointId:     1,
//				KColumnId:      1,
//				Name:           "timestamp",
//				SqlType:        *types.Timestamp,
//			},
//			{
//				KObjectTableId: 1,
//				EndpointId:     1,
//				KColumnId:      2,
//				Name:           "value",
//				SqlType:        *types.Int,
//			},
//			{
//				KObjectTableId: 1,
//				EndpointId:     1,
//				KColumnId:      2,
//				Name:           "valid",
//				SqlType:        *types.Bool,
//			},
//			{
//				KObjectTableId: 1,
//				EndpointId:     2,
//				KColumnId:      4,
//				Name:           "value",
//				SqlType:        *types.Int,
//			},
//			{
//				KObjectTableId: 1,
//				EndpointId:     2,
//				KColumnId:      5,
//				Name:           "valid",
//				SqlType:        *types.Bool,
//			},
//			{
//				KObjectTableId: 2,
//				EndpointId:     3,
//				KColumnId:      1,
//				Name:           "timestamp",
//				SqlType:        *types.Timestamp,
//			},
//			{
//				KObjectTableId: 2,
//				EndpointId:     3,
//				KColumnId:      2,
//				Name:           "value",
//				SqlType:        *types.Int,
//			},
//			{
//				KObjectTableId: 2,
//				EndpointId:     3,
//				KColumnId:      2,
//				Name:           "valid",
//				SqlType:        *types.Bool,
//			},
//			{
//				KObjectTableId: 2,
//				EndpointId:     4,
//				KColumnId:      4,
//				Name:           "value",
//				SqlType:        *types.Int,
//			},
//			{
//				KObjectTableId: 2,
//				EndpointId:     4,
//				KColumnId:      5,
//				Name:           "valid",
//				SqlType:        *types.Bool,
//			},
//		},
//	}
//	tableName := tree.MakeDeviceName("t1", "p1", "d1")
//	tabMeta := b.addTable(&device, &tableName)
//	endpCount := len(device.Endpoints[1])
//	var endpIDs opt.ColSet
//
//	for _, es := range device.Endpoints {
//		for i := 0; i < endpCount; i++ {
//			endpoint := es[i]
//			colID := opt.ColumnID(endpoint.EID)
//			for _, col := range endpoint.ZColMap {
//				endpIDs.Add(opt.ColumnID(col.ID))
//			}
//			name := tree.Name(endpoint.Name)
//			// build scopecolumn based on endpoint information and store it in scope.cols
//			outScope.cols = append(outScope.cols, scopeColumn{
//				id:             colID,
//				name:           name,
//				table:          tableName,
//				typ:            &endpoint.Datatype,
//				kColMap:        endpoint.ZColMap,
//				kColsName:      endpoint.ZCols,
//				attributes:     endpoint.Attributes,
//				kobjectTableID: endpoint.KObjTableID,
//			})
//		}
//	}
//	outScope.deviceList = []sqlbase.DeviceInfo{
//		{
//			SearchPath: [2]string{"t1", "p1"},
//			Device:     &device,
//		},
//	}
//	private := memo.ScanPrivate{Table: tabMeta.MetaID, Cols: endpIDs}
//	outScope.expr = b.factory.ConstructScan(&private)
//	return outScope
//}

//func newEndpointGroupScope() *scope {
//	outScope := &scope{}
//	device := sqlbase.DeviceDesc{
//		DeviceName: "d1",
//		Endpoints: map[uint64][]sqlbase.ZobjTableDesc{
//			1: {
//				{
//					Name:        "e1",
//					EndpointInfos:         1,
//					KObjTableID: 1,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {2, *types.Int},
//						"valid":     {3, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//				{
//					Name:        "e2",
//					EndpointInfos:         2,
//					KObjTableID: 1,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {4, *types.Int},
//						"valid":     {5, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//			},
//			2: {
//				{
//					Name:        "e3",
//					EndpointInfos:         3,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {2, *types.Int},
//						"valid":     {3, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//				{
//					Name:        "e4",
//					EndpointInfos:         4,
//					KObjTableID: 2,
//					ZColMap: map[string]*sqlbase.ZColInfo{
//						"timestamp": {1, *types.Int},
//						"value":     {4, *types.Int},
//						"valid":     {5, *types.Int},
//					},
//					ZCols:    []string{"timestamp", "value", "valid"},
//					Datatype: *types.Int,
//				},
//			},
//		},
//		Attributes:       map[string]string{},
//		PortalAttributes: map[string]string{},
//	}
//	device.Attributes["color"] = "red"
//	device.PortalAttributes["size"] = "10"
//	outScope.deviceList = append(outScope.deviceList, sqlbase.DeviceInfo{
//		SearchPath: [2]string{"t1", "p1"},
//		Device:     &device,
//	},
//	)
//
//	return outScope
//}
