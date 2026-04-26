package entity

import "time"

type SyncHistory struct {
	ID             string     `gorm:"primaryKey;size:36;column:id;default:(UUID())"`
	Status         string     `gorm:"column:status;type:enum('running','failed','succeded');not null;default:running"`
	StartDate      time.Time  `gorm:"column:start_date;not null"`
	EndDate        *time.Time `gorm:"column:end_date"`
	EndDateFilter  time.Time  `gorm:"column:end_date_filter;not null"`
	IsManualRunned bool       `gorm:"column:is_manual_runned;not null;default:false"`
	Error          *string    `gorm:"column:error;type:text"`
	CreatedAt      time.Time  `gorm:"column:created_at;not null"`
	UpdatedAt      time.Time  `gorm:"column:updated_at;not null"`
}

func (SyncHistory) TableName() string { return "sync_histories" }
