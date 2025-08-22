package lib

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ExecPlan represents the execution plan structure
type ExecPlan struct {
	Main             *PlanNode `json:"main"`
	WithRuntimeStats bool      `json:"withRuntimeStats"`
}

// PlanNode represents a single node in the execution plan
type PlanNode struct {
	Name              string   `json:"name"`
	ActRows           string   `json:"actRows,omitempty"`
	EstRows           float64  `json:"estRows,omitempty"`
	Cost              float64  `json:"cost,omitempty"`
	MemoryBytes       string   `json:"memoryBytes,omitempty"`
	DiskBytes         string   `json:"diskBytes,omitempty"`
	StoreType         string   `json:"storeType,omitempty"`
	TaskType          string   `json:"taskType,omitempty"`
	OperatorInfo      string   `json:"operatorInfo,omitempty"`
	RootBasicExecInfo string   `json:"rootBasicExecInfo,omitempty"`
	RootGroupExecInfo []string `json:"rootGroupExecInfo,omitempty"`
	CopExecInfo       string   `json:"copExecInfo,omitempty"`
	Labels            []string `json:"labels,omitempty"`

	AccessObjects []AccessObject `json:"accessObjects,omitempty"`

	Children []*PlanNode `json:"children,omitempty"`
}

// AccessObject represents an access object in the execution plan
type AccessObject struct {
	ScanObject *ScanObject `json:"scanObject,omitempty"`
}

type ScanObject struct {
	Database string       `json:"database,omitempty"`
	Table    string       `json:"table,omitempty"`
	Indexes  []IndexEntry `json:"indexes,omitempty"`
}

type IndexEntry struct {
	Name string   `json:"name,omitempty"`
	Cols []string `json:"cols,omitempty"`
}

func ParseExecPlan(data []byte) (*ExecPlan, error) {
	var plan ExecPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

// Walk traverses the execution plan tree and applies the provided function to each node
func (n *PlanNode) Walk(depth int, fn func(node *PlanNode, depth int)) {
	if n == nil {
		return
	}
	fn(n, depth)
	for _, child := range n.Children {
		child.Walk(depth+1, fn)
	}
}

func PrintExecPlan(plan *ExecPlan) {
	if plan == nil || plan.Main == nil {
		fmt.Println("Empty execution plan")
		return
	}

	fmt.Println("Execution Plan:")
	plan.Main.Walk(0, func(node *PlanNode, depth int) {
		fmt.Printf("%s- %s (actRows=%s, cost=%.2f)\n",
			strings.Repeat("  ", depth),
			node.Name, node.ActRows, node.Cost)
		if len(node.AccessObjects) > 0 {
			for _, ao := range node.AccessObjects {
				if ao.ScanObject != nil {
					fmt.Printf("%s  Scan: %s.%s\n", strings.Repeat("  ", depth),
						ao.ScanObject.Database, ao.ScanObject.Table)
				}
			}
		}
	})
}

func PrintExecPlanDetail(plan *ExecPlan) {
	// This function can be used to print detailed information about the execution plan
	// fields : all fields in PlanNode
	//          - Name
	//          - ActRows
	//          - EstRows
	//          - Cost
	//          - MemoryBytes
	//          - DiskBytes
	//          - StoreType
	//          - TaskType
	//          - OperatorInfo
	//          - RootBasicExecInfo
	//          - RootGroupExecInfo
	//          - CopExecInfo
	//          - Labels
	//          - AccessObjects
	//          - Children
	if plan == nil || plan.Main == nil {
		fmt.Println("Empty execution plan")
		return
	}
	fmt.Println("Detailed Execution Plan:")
	plan.Main.Walk(0, func(node *PlanNode, depth int) {
		fmt.Printf("%s- %s (actRows=%s, estRows=%.2f, cost=%.2f)\n",
			strings.Repeat("  ", depth),
			node.Name, node.ActRows, node.EstRows, node.Cost)
		if node.MemoryBytes != "" {
			fmt.Printf("%s  Memory: %s\n", strings.Repeat("  ", depth), node.MemoryBytes)
		}
		if node.DiskBytes != "" {
			fmt.Printf("%s  Disk: %s\n", strings.Repeat("  ", depth), node.DiskBytes)
		}
		if node.StoreType != "" {
			fmt.Printf("%s  Store Type: %s\n", strings.Repeat("  ", depth), node.StoreType)
		}
		if node.TaskType != "" {
			fmt.Printf("%s  Task Type: %s\n", strings.Repeat("  ", depth), node.TaskType)
		}
		if node.OperatorInfo != "" {
			fmt.Printf("%s  Operator Info: %s\n", strings.Repeat("  ", depth), node.OperatorInfo)
		}
		if node.RootBasicExecInfo != "" {
			fmt.Printf("%s  Root Basic Exec Info: %s\n", strings.Repeat("  ", depth), node.RootBasicExecInfo)
		}
		if len(node.RootGroupExecInfo) > 0 {
			fmt.Printf("%s  Root Group Exec Info: %v\n", strings.Repeat("  ", depth), node.RootGroupExecInfo)
		}
		if node.CopExecInfo != "" {
			fmt.Printf("%s  Cop Exec Info: %s\n", strings.Repeat("  ", depth), node.CopExecInfo)
		}
		if len(node.Labels) > 0 {
			fmt.Printf("%s  Labels: %v\n", strings.Repeat("  ", depth), node.Labels)
		}
		if len(node.AccessObjects) > 0 {
			for _, ao := range node.AccessObjects {
				if ao.ScanObject != nil {
					fmt.Printf("%s  Scan: %s.%s\n", strings.Repeat("  ", depth),
						ao.ScanObject.Database, ao.ScanObject.Table)
					for _, idx := range ao.ScanObject.Indexes {
						fmt.Printf("%s    Index: %s (cols: %v)\n", strings.Repeat("  ", depth+1),
							idx.Name, idx.Cols)
					}
				}
			}
		}
		if len(node.Children) > 0 {
			fmt.Printf("%s  Children:\n", strings.Repeat("  ", depth))
			for _, child := range node.Children {
				child.Walk(depth+1, func(childNode *PlanNode, childDepth int) {
					fmt.Printf("%s- %s (actRows=%s, cost=%.2f)\n",
						strings.Repeat("  ", childDepth),
						childNode.Name, childNode.ActRows, childNode.Cost)
				})
			}
		}

		fmt.Println("Execution plan detail printed successfully.")
	})
}
