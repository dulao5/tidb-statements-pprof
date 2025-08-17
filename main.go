package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	// Adjust import path as needed
	lib "tidb_statements_pprof/lib"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/pprof/profile"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <mysql_dsn|statements_csv> <output.pb>")
		fmt.Println(`Example DSN: 'root:@tcp(127.0.0.1:4000)/'`)
		return
	}

	dsn_or_csv := os.Args[1]
	outputFile := os.Args[2]

	p := &profile.Profile{
		SampleType: []*profile.ValueType{
			{Type: "latency", Unit: "nanoseconds"},
			{Type: "processed_keys", Unit: "count"},
			{Type: "cop_tasks", Unit: "count"},
			{Type: "process_time", Unit: "nanoseconds"},
			{Type: "resource_unit", Unit: "count"},
		},
		TimeNanos:     time.Now().UnixNano(),
		DurationNanos: int64(time.Second),
	}

	data := []lib.Row{}
	if strings.HasSuffix(dsn_or_csv, ".csv") {
		// Read from CSV file
		err := lib.GetDataFromCSV(dsn_or_csv, &data)
		if err != nil {
			panic(fmt.Errorf("failed to get data from CSV: %w", err))
		}
	} else {
		// Read from MySQL
		err := lib.GetDataFromMySQL(dsn_or_csv, &data)
		if err != nil {
			panic(fmt.Errorf("failed to get data from MySQL: %w", err))
		}
	}

	// Process each row and create pprof profile samples
	funcID := uint64(1)
	locID := uint64(1)
	funcMap := map[string]*profile.Function{}
	locMap := map[string]*profile.Location{}
	for _, r := range data {
		action := ""
		if parts := strings.Fields(r.DigestText); len(parts) > 0 {
			action = parts[0]
		}

		// pprof 调用栈从下到上，故最上层放 digest，最下层放 schema
		stack := []string{
			"digest:" + r.Digest,
			"action:" + action,
			"table:" + r.TableNames,
			"schema:" + r.SchemaName,
		}

		var locs []*profile.Location
		for _, fnName := range stack {
			fn, ok := funcMap[fnName]
			if !ok {
				fn = &profile.Function{
					ID:         funcID,
					Name:       fnName,
					SystemName: fnName,
					Filename:   fnName + ".sql",
				}
				funcID++
				funcMap[fnName] = fn
				p.Function = append(p.Function, fn)
			}

			loc, ok := locMap[fnName]
			if !ok {
				loc = &profile.Location{
					ID: locID,
					Line: []profile.Line{
						{Function: fn, Line: 1},
					},
				}
				locID++
				locMap[fnName] = loc
				p.Location = append(p.Location, loc)
			}
			locs = append(locs, loc)
		}

		sample := &profile.Sample{
			Location: locs,
			Value: []int64{
				int64(r.SumLatency * 1e9),  // seconds → nanoseconds
				int64(r.SumProcKeys),       // count
				int64(r.SumCopTasks),       // count
				int64(r.SumProcTime * 1e9), // seconds → nanoseconds
				int64(r.SUMRU),             // resource unit
			},
		}
		p.Sample = append(p.Sample, sample)
	}

	if err := p.CheckValid(); err != nil {
		panic(fmt.Errorf("invalid profile: %w", err))
	}

	out, err := os.Create(outputFile)
	if err != nil {
		panic(err)
	}
	defer out.Close()

	// 直接写 protobuf，不要 gzip，go tool pprof 自动识别支持
	if err := p.Write(out); err != nil {
		panic(err)
	}

	fmt.Println("✅ pprof file generated:", outputFile)
	fmt.Println("View with: go tool pprof -http=:8080", outputFile)
}
