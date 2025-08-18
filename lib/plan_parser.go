package lib

import (
	"encoding/json"
	"regexp"
	"strings"
)

type PlanNode struct {
	ID            string
	TaskType      string
	Depth         int
	IsLeaf        bool
	Task          string
	EstRows       string
	OperatorInfo  string
	ActRows       string
	ExecutionInfo string
	Memory        string
	Disk          string
	Table         string
	Index         string
}

func ParsePlan(plan string) ([]PlanNode, error) {
	var nodes []PlanNode
	lines := strings.Split(plan, "\n")
	if len(lines) < 2 {
		return nodes, nil
	}

	header := strings.Split(lines[0], "\t")
	for _, line := range lines[1:] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		values := strings.Split(line, "\t")
		node := PlanNode{}
		for i, value := range values {
			if i < len(header) {
				switch strings.TrimSpace(header[i]) {
				case "id":
					node.ID = value
				case "task":
					node.Task = value
				case "estRows":
					node.EstRows = value
				case "operator info":
					node.OperatorInfo = value
				case "actRows":
					node.ActRows = value
				case "execution info":
					node.ExecutionInfo = value
				case "memory":
					node.Memory = value
				case "disk":
					node.Disk = value
				}
			}
		}
		nodes = append(nodes, node)
	}

	for i := range nodes {
		parseID(&nodes[i])
		parseOperatorInfo(&nodes[i])
	}

	return nodes, nil
}

func parseID(node *PlanNode) {
	trimmedID := strings.TrimLeft(node.ID, " └─")
	re := regexp.MustCompile(`^([a-zA-Z_]+)(_[0-9]+)`)
	matches := re.FindStringSubmatch(trimmedID)
	if len(matches) > 1 {
		node.TaskType = matches[1]
	}

	if strings.Contains(node.ID, "└─") {
		prefix := strings.Split(node.ID, "└─")[0]
		node.Depth = len(prefix) / 2
	} else {
		node.Depth = 0
	}
	node.IsLeaf = !strings.Contains(node.ID, "└─")
}

func parseOperatorInfo(node *PlanNode) {
	opInfo := node.OperatorInfo
	if strings.Contains(opInfo, "table:") {
		re := regexp.MustCompile(`table:([^,]+)`)
		matches := re.FindStringSubmatch(opInfo)
		if len(matches) > 1 {
			node.Table = matches[1]
		}
	}
	if strings.Contains(opInfo, "index:") {
		re := regexp.MustCompile(`index:([^,]+)`)
		matches := re.FindStringSubmatch(opInfo)
		if len(matches) > 1 {
			node.Index = matches[1]
		}
	}
}

type ExecInfo struct {
	Time      string `json:"time"`
	Loops     string `json:"loops"`
	NumRPC    string `json:"num_rpc"`
	TotalTime string `json:"total_time"`
}

func ParseExecInfo(execInfo string) (ExecInfo, error) {
	var info ExecInfo
	err := json.Unmarshal([]byte(execInfo), &info)
	return info, err
}
