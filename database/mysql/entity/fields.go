package mysql

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
	AirbyteFieldFieldSortOrder    = "sort_order"
	AirbyteFieldFieldCreatedAt    = "created_at"
	AirbyteFieldFieldUpdatedAt    = "updated_at"
)
