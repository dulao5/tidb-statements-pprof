package lib

import (
	"database/sql"
)

// getDataFromMySQL get data from mysql
func GetDataFromMySQL(dsn string, data *[]Row) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	query := `
SELECT IFNULL(SCHEMA_NAME, "null") AS SCHEMA_NAME,
       IFNULL(TABLE_NAMES, "null") AS TABLE_NAMES,
       DIGEST_TEXT, DIGEST,
       SUM_LATENCY/1e9 AS sum_latency_sec,
       AVG_PROCESSED_KEYS * EXEC_COUNT AS sum_processed_keys,
       SUM_COP_TASK_NUM AS sum_cop_tasks,
       (AVG_PROCESS_TIME/1e9) * EXEC_COUNT AS sum_tikv_process_time_sec,
	   (AVG_REQUEST_UNIT_READ+ AVG_REQUEST_UNIT_WRITE) * EXEC_COUNT AS sum_ru
FROM information_schema.cluster_statements_summary;
`
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.SchemaName, &r.TableNames, &r.DigestText, &r.Digest,
			&r.SumLatency, &r.SumProcKeys, &r.SumCopTasks, &r.SumProcTime, &r.SUMRU); err != nil {
			return err
		}
		*data = append(*data, r)
	}
	return nil
}
