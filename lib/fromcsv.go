package lib

import (
	"encoding/csv"
	"os"

	// parseFloat is a helper function to convert string to float64
	"strconv"
)

// getDataFromCSV get data from csv file
func GetDataFromCSV(csvfile string, data *[]Row) error {
	// Open the CSV file
	file, err := os.Open(csvfile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)
	reader.Comma = ',' // Set the delimiter to comma

	// Read all records from the CSV file
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	// get first row as header
	header := records[0]

	// second row is the data
	for _, record := range records[1:] {
		var rowHashMap = make(map[string]string)
		for i, h := range header {
			rowHashMap[h] = record[i]
		}
		// Convert string values to appropriate types
		sumLatency, _ := strconv.ParseFloat(rowHashMap["sum_latency"], 64)
		sumLatency /= 1e9 // Convert nanoseconds to seconds
		sumCopTasks, _ := strconv.ParseFloat(rowHashMap["sum_cop_task_num"], 64)
		avgProcKeys, _ := strconv.ParseFloat(rowHashMap["avg_processed_keys"], 64)
		avgProcTime, _ := strconv.ParseFloat(rowHashMap["avg_process_time"], 64)
		avgProcTime /= 1e9 // Convert nanoseconds to seconds
		execCount, _ := strconv.ParseInt(rowHashMap["exec_count"], 10, 64)
		sumProcKeys := avgProcKeys * float64(execCount)
		sumProcTime := avgProcTime * float64(execCount)
		sumRU, _ := strconv.ParseFloat(rowHashMap["sum_ru"], 64)

		maxLatency, _ := strconv.ParseFloat(rowHashMap["max_latency"], 64)
		maxLatency /= 1e6 // Convert nanoseconds to Milliseconds
		avgLatency, _ := strconv.ParseFloat(rowHashMap["avg_latency"], 64)
		avgLatency /= 1e6 // Convert nanoseconds to Milliseconds

		// Create a Row instance and append it to the data slice

		r := Row{
			SchemaName:   rowHashMap["schema_name"],
			TableNames:   rowHashMap["table_names"],
			DigestText:   rowHashMap["digest_text"],
			Digest:       rowHashMap["digest"],
			SumLatency:   sumLatency,
			SumProcKeys:  sumProcKeys,
			SumCopTasks:  sumCopTasks,
			SumProcTime:  sumProcTime,
			SUMRU:        sumRU,
			ExecCount:    execCount,
			MaxLatencyMs: maxLatency,
			AvgLatencyMs: avgLatency,
		}
		*data = append(*data, r)
	}
	return nil
}
