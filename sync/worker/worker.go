package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"airbyte-service/core"
	mongodatabase "airbyte-service/database/mongo"
	"airbyte-service/database/postgres"
	syncdestination "airbyte-service/sync/destination"
	sourcecommon "airbyte-service/sync/sources/common"
	sourcemongo "airbyte-service/sync/sources/mongo"
)

type Table struct {
	Name       string
	WriteMode  core.WriteMode
	PrimaryKey string
	Fields     []sourcecommon.FieldSpec
}

type Source struct {
	MongoURI string
	Database string
	Tables   []Table
}

// func (s *Scheduler) LoadDatabasesWithTables() {
// 	dbList, err := s.databaseService.GetList()
// 	if err != nil {
// 		log.Fatalf("load databases: %v", err)
// 	}

// 	var sources []Source
// 	for _, db := range dbList.Databases {
// 		dbID := db.ID
// 		tableList, err := s.tableService.GetList(&dbID)
// 		if err != nil {
// 			log.Fatalf("load tables for db %s: %v", db.Name, err)
// 		}

// 		var tables []Table
// 		for _, tbl := range tableList.Tables {
// 			tblID := tbl.ID
// 			fieldList, err := s.fieldService.GetList(&tblID)
// 			if err != nil {
// 				log.Fatalf("load fields for table %s: %v", tbl.Name, err)
// 			}

// 			specs := make([]sourcecommon.FieldSpec, len(fieldList.Fields))
// 			for i, f := range fieldList.Fields {
// 				specs[i] = sourcecommon.FieldSpec{
// 					Name:          f.FieldName,
// 					PgType:        f.PgType,
// 					SeparateTable: f.IsChildTable != nil && *f.IsChildTable,
// 				}
// 			}

// 			tables = append(tables, Table{
// 				Name:       tbl.Name,
// 				WriteMode:  core.WriteMode(tbl.WriteMode),
// 				PrimaryKey: tbl.PrimaryKey,
// 				Fields:     specs,
// 			})
// 		}

// 		sources = append(sources, Source{
// 			MongoURI: db.URI,
// 			Database: db.Name,
// 			Tables:   tables,
// 		})
// 	}
// 	s.sources = sources
// }

func (s *Scheduler) PutDatabasesWithTables(sources []Source) {
	s.sources = sources
}

func (s *Scheduler) runSync(ctx context.Context) error {
	for _, src := range s.sources {
		if err := s.syncSource(ctx, src); err != nil {
			return fmt.Errorf("source %s: %w", src.Database, err)
		}
	}
	return nil
}

func (s *Scheduler) syncSource(ctx context.Context, src Source) error {
	mongoCli, err := mongodatabase.NewDatabase(mongodatabase.MongoConfig{ConnectionString: src.MongoURI})
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer mongoCli.Disconnect()

	mongoCat, err := sourcemongo.NewMongoDiscoverer(mongoCli.GetClient(), src.Database, 1_000).Discover(ctx)
	if err != nil {
		return fmt.Errorf("discover schema: %w", err)
	}

	for _, tbl := range src.Tables {
		if err := s.syncTable(ctx, mongoCli, mongoCat, src.Database, tbl); err != nil {
			return fmt.Errorf("table %s: %w", tbl.Name, err)
		}
	}
	return nil
}

func (s *Scheduler) syncTable(ctx context.Context, mongoCli mongodatabase.Database, mongoCat *sourcecommon.DatabaseScheme, database string, tbl Table) error {
	discovered, ok := mongoCat.Get(database, tbl.Name)
	if !ok {
		return fmt.Errorf("table %q not found in database scheme", tbl.Name)
	}
	table := discovered.FilterFields(tbl.Fields)
	table.FillTableNames()

	if err := postgres.EnsureTable(ctx, s.pool, table); err != nil {
		return fmt.Errorf("ensure tables: %w", err)
	}

	msgCh, err := sourcemongo.ReadCollection(ctx, mongoCli.GetClient(), database, tbl.Name, table)
	if err != nil {
		return fmt.Errorf("read table: %w", err)
	}

	writers := make(map[string]*syncdestination.Writer)
	channels := make(map[string]chan sourcemongo.Row)

	for _, tableName := range table.Tables {
		ch := make(chan sourcemongo.Row, 256)
		channels[tableName] = ch
		writers[tableName] = syncdestination.NewWriter(s.pool, tableName, tbl.WriteMode, []string{tbl.PrimaryKey}, slog.Default())
	}

	go func() {
		for msg := range msgCh {
			if ch, ok := channels[msg.Table]; ok {
				ch <- msg.Row
			}
		}
		for _, ch := range channels {
			close(ch)
		}
	}()

	var wg sync.WaitGroup
	results := make(map[string]*syncdestination.WriteResult)
	var mu sync.Mutex

	for tableName, w := range writers {
		wg.Add(1)
		go func(name string, w *syncdestination.Writer, ch <-chan sourcemongo.Row) {
			defer wg.Done()
			res, err := w.Write(ctx, table, ch)
			if err != nil {
				s.logger.Error("write failed", "db", database, "table", tbl.Name, "dest_table", name, "err", err)
				return
			}
			mu.Lock()
			results[name] = res
			mu.Unlock()
		}(tableName, w, channels[tableName])
	}

	wg.Wait()

	for name, res := range results {
		s.logger.Info("sync done",
			"db", database,
			"table", tbl.Name,
			"dest_table", name,
			"rows", res.RowsCopied,
			"batches", res.Batches,
			"duration", res.Duration,
		)
	}

	return nil
}
