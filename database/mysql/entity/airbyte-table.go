package entity

import "time"

type AirbyteTable struct {
	ID               string    `gorm:"primaryKey;size:36;column:id;default:(UUID())"`
	DatabaseID       string    `gorm:"column:database_id;size:36;not null;index:idx_tables_database_id"`
	Name             string    `gorm:"column:name;size:255;not null"`
	WriteMode        string    `gorm:"column:write_mode;type:enum('append','overwrite','upsert');not null;default:upsert"`
	PrimaryKey       string    `gorm:"column:primary_key;size:255;not null;default:id"`
	IsActive         bool      `gorm:"column:is_active;not null;default:false"`
	CreatedTimeField string    `gorm:"column:created_time_field;size:255;not null"`
	UpdatedTimeField string    `gorm:"column:updated_time_field;size:255;not null"`
	DeletedTimeField string    `gorm:"column:deleted_time_field;size:255;not null"`
	CreatedAt        time.Time `gorm:"column:created_at;not null"`
	UpdatedAt        time.Time `gorm:"column:updated_at;not null"`
}

func (AirbyteTable) TableName() string { return "tables" }
