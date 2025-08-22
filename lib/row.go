package lib

import (
	"regexp"
	"strings"
)

type Row struct {
	SchemaName   string
	TableNames   string
	DigestText   string
	Digest       string
	SumLatency   float64
	SumProcKeys  float64
	SumCopTasks  float64
	SumProcTime  float64
	SUMRU        float64
	ExecCount    int64
	MaxLatencyMs float64
	AvgLatencyMs float64
}

// summarizeSQL summarizes a SQL statement into a more shortable format.
func (row *Row) SummarizeSQL() string {
	// replace backticks
	s := strings.ToLower(strings.TrimSpace(row.DigestText))
	s = strings.ReplaceAll(s, "`", "")

	// delete between "Select" and "from"
	if strings.HasPrefix(s, "select") {
		re := regexp.MustCompile(`select\s+.*?\s+from`)
		s = re.ReplaceAllString(s, "select ... from")
	}

	// replace insert values to ... after "values"
	if strings.HasPrefix(s, "insert") {
		re := regexp.MustCompile(`values\s+.*`)
		s = re.ReplaceAllString(s, "values ...")
	}

	// delete between "set" and "where"
	if strings.HasPrefix(s, "update") {
		re := regexp.MustCompile(`set\s+.*?\s+where`)
		s = re.ReplaceAllString(s, "set ... where")
	}
	return s
}
