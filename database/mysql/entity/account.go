package mysql

import (
	"time"

	"gorm.io/gorm"
)

type Account struct {
	ID           string     `gorm:"primaryKey;size:24;column:id"`
	CreatedAt    time.Time  `gorm:"column:created_at;not null"`
	UpdatedAt    time.Time  `gorm:"column:updated_at;not null"`
	DeletedAt    *time.Time `gorm:"column:deleted_at"`
	IsDeleted    bool       `gorm:"column:is_deleted;not null"`
	Username     string     `gorm:"column:username;size:12;not null;index:idx_username,priority:1"`
	PasswordHash string     `gorm:"column:password_hash;size:100;not null"`
	Name         string     `gorm:"column:name;size:200;not null"`
	Type         byte       `gorm:"column:type;not null"`
	Role         *int       `gorm:"column:role;"`
	IsBlocked    bool       `gorm:"column:is_blocked;not null"`
	RegionCode   *int       `gorm:"column:region_code;"`
	DistrictCode *int       `gorm:"column:district_code;"`
}

func (Account) TableName() string {
	return "accounts"
}

func (Account) FilterUsername(username string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("accounts.username = ?", username)
	}
}

func (Account) FilterIsDeleted(value bool) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("accounts.is_deleted = ?", value)
	}
}

func (Account) FilterID(id string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("accounts.id = ?", id)
	}
}
