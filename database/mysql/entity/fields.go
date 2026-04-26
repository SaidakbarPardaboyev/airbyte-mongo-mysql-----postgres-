package entity

// databases
const (
	AirbyteDatabaseFieldID        = "id"
	AirbyteDatabaseFieldName      = "name"
	AirbyteDatabaseFieldURI       = "uri"
	AirbyteDatabaseFieldCreatedAt = "created_at"
	AirbyteDatabaseFieldUpdatedAt = "updated_at"
)

// tables
const (
	AirbyteTableFieldID         = "id"
	AirbyteTableFieldDatabaseID = "database_id"
	AirbyteTableFieldName       = "name"
	AirbyteTableFieldWriteMode  = "write_mode"
	AirbyteTableFieldPrimaryKey = "primary_key"
	AirbyteTableFieldIsActive   = "is_active"
	AirbyteTableFieldCreatedAt  = "created_at"
	AirbyteTableFieldUpdatedAt  = "updated_at"
)

// fields
const (
	AirbyteFieldFieldID           = "id"
	AirbyteFieldFieldTableID      = "table_id"
	AirbyteFieldFieldName         = "field_name"
	AirbyteFieldFieldPgType       = "pg_type"
	AirbyteFieldFieldIsChildTable = "is_child_table"
	AirbyteFieldFieldCreatedAt    = "created_at"
	AirbyteFieldFieldUpdatedAt    = "updated_at"
)

// sync_histories
const (
	SyncHistoryFieldID             = "id"
	SyncHistoryFieldStatus         = "status"
	SyncHistoryFieldStartDate      = "start_date"
	SyncHistoryFieldEndDate        = "end_date"
	SyncHistoryFieldEndDateFilter  = "end_date_filter"
	SyncHistoryFieldIsManualRunned = "is_manual_runned"
	SyncHistoryFieldError          = "error"
	SyncHistoryFieldCreatedAt      = "created_at"
	SyncHistoryFieldUpdatedAt      = "updated_at"
)
