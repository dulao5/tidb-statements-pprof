package lib

type Row struct {
	SchemaName  string
	TableNames  string
	DigestText  string
	Digest      string
	SumLatency  float64
	SumProcKeys float64
	SumCopTasks float64
	SumProcTime float64
	SUMRU       float64 // Resource unit, if applicable
}
