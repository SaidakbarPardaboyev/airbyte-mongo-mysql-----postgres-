package core

import "time"

type WriteMode string

const (
	WriteModeAppend    WriteMode = "append"    // COPY straight into target table
	WriteModeOverwrite WriteMode = "overwrite" // TRUNCATE then COPY
	WriteModeUpsert    WriteMode = "upsert"    // COPY into temp, then MERGE/INSERT ON CONFLICT
)

const (
	BatchSize = 5_000
	Timeout   = 5 * time.Minute
)
