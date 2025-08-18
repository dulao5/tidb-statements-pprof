package lib

import (
	"strings"
	"testing"
)

func TestParsePlan(t *testing.T) {
	plan := `	id                       	task     	estRows	operator info                                                          	actRows	execution info                                                                                                                                                                                                                                                                                                                                                                                           	memory 	disk
	Sort_6                   	root     	82.15  	test.sbtest2.c                                                         	100    	time:1.06ms, loops:2                                                                                                                                                                                                                                                                                                                                                                                     	16.3 KB	0 Bytes
	└─HashAgg_12             	root     	82.15  	group by:test.sbtest2.c, funcs:firstrow(test.sbtest2.c)->test.sbtest2.c	100    	time:959.6µs, loops:6, partial_worker:{wall_time:936.916µs, concurrency:5, task_num:1, tot_wait:829.166µs, tot_exec:76.583µs, tot_time:4.627502ms, max:927.584µs, p95:927.584µs}, final_worker:{wall_time:957.875µs, concurrency:5, task_num:5, tot_wait:2.459µs, tot_exec:165ns, tot_time:4.711459ms, max:946.834µs, p95:946.834µs}                                                           	97.0 KB	0 Bytes
	  └─TableReader_13       	root     	82.15  	data:HashAgg_8                                                         	100    	time:831.3µs, loops:2, cop_task: {num: 1, max: 802.4µs, proc_keys: 100, tot_proc: 584.2µs, tot_wait: 22.2µs, copr_cache_hit_ratio: 0.00, build_task_duration: 3.79µs, max_distsql_concurrency: 1}, rpc_info:{Cop:{num_rpc:1, total_time:796µs}}                                                                                                                                                    	12.7 KB	N/A
	    └─HashAgg_8          	cop[tikv]	82.15  	group by:test.sbtest2.c,                                               	100    	tikv_task:{time:1ms, loops:1}, scan_detail: {total_process_keys: 100, total_process_keys_size: 22396, total_keys: 101, get_snapshot_time: 11.8µs, rocksdb: {key_skipped_count: 100, block: {cache_hit_count: 1, read_count: 1, read_byte: 12.9 KB, read_time: 177.7µs}}}, time_detail: {total_process_time: 584.2µs, total_wait_time: 22.2µs, total_kv_read_wall_time: 1ms, tikv_wall_time: 678.8µs}	N/A    	N/A
	      └─TableRangeScan_11	cop[tikv]	102.68 	table:sbtest2, range:[939326,939425], keep order:false                 	100    	tikv_task:{time:1ms, loops:1}                                                                                                                                                                                                                                                                                                                                                                            	N/A    	N/A`

	nodes, err := ParsePlan(plan)
	if err != nil {
		t.Fatalf("ParsePlan failed: %v", err)
	}

	if len(nodes) != 5 {
		t.Fatalf("expected 5 nodes, got %d", len(nodes))
	}

	if nodes[0].TaskType != "Sort" {
		t.Errorf("expected TaskType to be 'Sort', got '%s'", nodes[0].TaskType)
	}

	if nodes[0].Depth != 0 {
		t.Errorf("expected Depth to be 0, got %d", nodes[0].Depth)
	}

	if nodes[1].TaskType != "HashAgg" {
		t.Errorf("expected TaskType to be 'HashAgg', got '%s'", nodes[1].TaskType)
	}

	if nodes[1].Depth != 0 {
		t.Errorf("expected Depth to be 0, got %d", nodes[1].Depth)
	}

	if nodes[4].Table != "sbtest2" {
		t.Errorf("expected Table to be 'sbtest2', got '%s'", nodes[4].Table)
	}
}

func TestParseID(t *testing.T) {
	tests := []struct {
		id       string
		taskType string
		depth    int
		isLeaf   bool
	}{
		{"Sort_6", "Sort", 0, true},
		{"└─HashAgg_12", "HashAgg", 0, false},
		{"  └─TableReader_13", "TableReader", 1, false},
		{"    └─HashAgg_8", "HashAgg", 2, false},
		{"      └─TableRangeScan_11", "TableRangeScan", 3, false},
	}

	for _, tt := range tests {
		node := &PlanNode{ID: tt.id}
		parseID(node)
		if node.TaskType != tt.taskType {
			t.Errorf("for id '%s', expected TaskType '%s', got '%s'", tt.id, tt.taskType, node.TaskType)
		}
		if node.Depth != tt.depth {
			t.Errorf("for id '%s', expected Depth %d, got %d'", tt.id, tt.depth, node.Depth)
		}
		if node.IsLeaf != tt.isLeaf {
			t.Errorf("for id '%s', expected IsLeaf %t, got %t'", tt.id, tt.isLeaf, node.IsLeaf)
		}
	}
}

func TestParseOperatorInfo(t *testing.T) {
	tests := []struct {
		operatorInfo string
		table        string
		index        string
	}{
		{"table:sbtest2, handle:548969", "sbtest2", ""},
		{"table:sbtest2, index:k_2", "sbtest2", "k_2"},
	}

	for _, tt := range tests {
		node := &PlanNode{OperatorInfo: tt.operatorInfo}
		parseOperatorInfo(node)
		if node.Table != tt.table {
			t.Errorf("for operator info '%s', expected Table '%s', got '%s'", tt.operatorInfo, tt.table, node.Table)
		}
		if node.Index != tt.index {
			t.Errorf("for operator info '%s', expected Index '%s', got '%s'", tt.operatorInfo, tt.index, node.Index)
		}
	}
}

func TestParseExecInfo(t *testing.T) {
	execInfo := `{"time":"1.68ms", "loops":"2", "Get":{"num_rpc":"1", "total_time":"1.67ms"}}`
	info, err := ParseExecInfo(execInfo)
	if err != nil {
		if !strings.Contains(err.Error(), "json: unknown field") {
			t.Fatalf("ParseExecInfo failed: %v", err)
		}
	}
	if info.Time != "1.68ms" {
		t.Errorf("expected Time to be '1.68ms', got '%s'", info.Time)
	}
}
