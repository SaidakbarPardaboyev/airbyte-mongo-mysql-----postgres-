package entity

import "time"

type AirbyteField struct {
	ID              string    `gorm:"primaryKey;size:36;column:id;default:(UUID())"`
	TableID         string    `gorm:"column:table_id;size:36;not null;index:idx_fields_table_id"`
	FieldName       string    `gorm:"column:field_name;size:512;not null"`
	DestinationType string    `gorm:"column:destination_type;size:64;not null;default:''"`
	Nullable        bool      `gorm:"column:nullable;not null;default:false"`
	IsPrimary       bool      `gorm:"column:primary_key;not null;default:false"`
	IsChildTable    *bool     `gorm:"column:is_child_table;default:null"`
	CreatedAt       time.Time `gorm:"column:created_at;not null"`
	UpdatedAt       time.Time `gorm:"column:updated_at;not null"`
}

func (AirbyteField) TableName() string { return "fields" }
