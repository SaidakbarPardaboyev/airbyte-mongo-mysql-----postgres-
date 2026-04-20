package mysql

import "time"

type AirbyteField struct {
	ID           string    `gorm:"primaryKey;size:36;column:id;default:(UUID())"`
	TableID      string    `gorm:"column:table_id;size:36;not null;index:idx_fields_table_id"`
	FieldName    string    `gorm:"column:field_name;size:512;not null"`
	PgType       string    `gorm:"column:pg_type;size:64;not null;default:''"`
	IsChildTable *bool     `gorm:"column:is_child_table;default:null"`
	SortOrder    int       `gorm:"column:sort_order;not null;default:0"`
	CreatedAt    time.Time `gorm:"column:created_at;not null"`
	UpdatedAt    time.Time `gorm:"column:updated_at;not null"`
}

func (AirbyteField) TableName() string { return "fields" }
