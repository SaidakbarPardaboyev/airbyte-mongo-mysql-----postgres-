package mysql

import (
	"fmt"
	"net/url"
	"strings"

	"airbyte-service/plugins"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type database struct {
	db *gorm.DB
}

func (d *database) GetDB() *gorm.DB {
	return d.db
}

func (d *database) Migrate() error {
	// return d.db.AutoMigrate(Account{}, ...)
	return nil
}

type Database interface {
	Migrate() error
	GetDB() *gorm.DB
}

func New(username, password, addr, databaseName, timezone string) (Database, error) {
	var paramString = encodeMap(map[string]string{
		"charset":   "utf8mb4",
		"parseTime": "True",
		"loc":       timezone,
	})
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", username, password, addr, databaseName, paramString)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: true,
		NowFunc:                plugins.GetNow,
		Logger:                 logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, err
	}

	return &database{
		db: db,
	}, nil
}

func encodeMap(m map[string]string) string {
	var encodedPairs []string
	for key, value := range m {
		encodedKey := url.QueryEscape(key)
		encodedValue := url.QueryEscape(value)
		encodedPairs = append(encodedPairs, fmt.Sprintf("%s=%s", encodedKey, encodedValue))
	}
	return strings.Join(encodedPairs, "&")
}
