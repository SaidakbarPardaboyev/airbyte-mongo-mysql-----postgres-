package mysql

import "time"

type AirbyteDatabase struct {
	ID        string    `gorm:"primaryKey;size:36;column:id;default:(UUID())"`
	Name      string    `gorm:"column:name;size:255;not null"`
	URI       string    `gorm:"column:uri;type:text;not null"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null"`
}

func (AirbyteDatabase) TableName() string { return "databases" }
