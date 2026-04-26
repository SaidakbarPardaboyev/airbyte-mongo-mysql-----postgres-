package worker

import (
	"context"
	"fmt"

	// "log"
	"log/slog"
	"sync"

	// "airbyte-service/core"
	mongodatabase "airbyte-service/database/mongo"
	// "airbyte-service/database/mysql/entity"
	"airbyte-service/database/postgres"
	syncdestination "airbyte-service/sync/destination"
	sourcecommon "airbyte-service/sync/sources/common"
	sourcemongo "airbyte-service/sync/sources/mongo"
)

func (s *Scheduler) runSync(ctx context.Context, sourcesFromDatabase []sourcecommon.DatabaseScheme) (err error) {
	for _, sourceFromDatabase := range sourcesFromDatabase {
		// initialize mongo cli
		var mongoCli mongodatabase.Database
		{
			mongoCli, err = mongodatabase.NewDatabase(mongodatabase.MongoConfig{ConnectionString: sourceFromDatabase.MongoURI()})
			if err != nil {
				return fmt.Errorf("connect mongo: %w", err)
			}
		}
		defer mongoCli.Disconnect()

		// load database scheme from the connection
		var discoveredMongoScheme sourcecommon.DatabaseScheme
		{
			discoveredMongoScheme, err = sourcemongo.NewMongoDiscoverer(mongoCli.GetClient(), sourceFromDatabase.Database(), 1_000).Discover(ctx)
			if err != nil {
				return fmt.Errorf("discover schema: %w", err)
			}
		}

		// sync each table
		for _, table := range sourceFromDatabase.Tables() {
			// get discovered table
			discoveredTable, ok := discoveredMongoScheme.Get(sourceFromDatabase.Database(), table.Name)
			if !ok {
				return fmt.Errorf("table %q not found in database scheme", table.Name)
			}

			// filter fields (take only asked fields)
			discoveredTable.FilterFields(table)
			discoveredTable.FillTableNames()

			if err := s.syncTable(ctx, mongoCli, discoveredTable, sourceFromDatabase.Database()); err != nil {
				return fmt.Errorf("table %s: %w", table.Name, err)
			}
		}
	}
	return nil
}

func (s *Scheduler) syncTable(ctx context.Context, mongoCli mongodatabase.Database, discoveredTable *sourcecommon.Table, dbName string) (err error) {
	// if table not create in destination, create it
	{
		if err := postgres.EnsureTable(ctx, s.destination, discoveredTable); err != nil {
			return fmt.Errorf("ensure tables: %w", err)
		}
	}

	// load data
	var msgCh <-chan sourcemongo.Message
	{
		msgCh, err = sourcemongo.ReadCollection(ctx, mongoCli.GetClient(), dbName, discoveredTable, s.lastSyncEndTimeFilter, s.currentSyncEndingTimeFilter)
		if err != nil {
			return fmt.Errorf("read table: %w", err)
		}
	}

	// create maps for writing paralel
	//    > for writer of each table
	//    > for channel of each table
	writers := make(map[string]*syncdestination.Writer)
	channels := make(map[string]chan sourcemongo.Row)
	{
		for _, tableName := range discoveredTable.Tables {
			ch := make(chan sourcemongo.Row, 256)

			channels[tableName] = ch
			writers[tableName] = syncdestination.NewWriter(s.destination, tableName, discoveredTable.WriteMode, slog.Default())
		}
	}

	// send batch data to the correct table's channel
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

	// listen each channel and write to correct table
	{
		var wg sync.WaitGroup
		errs := make(chan error, len(writers))

		for tableName, w := range writers {
			wg.Add(1)
			go func(name string, w *syncdestination.Writer, ch <-chan sourcemongo.Row) {
				defer wg.Done()
				res, err := w.Write(ctx, discoveredTable, ch)
				if err != nil {
					errs <- fmt.Errorf("write %s/%s dest_table=%s: %w", dbName, discoveredTable.Name, name, err)
					return
				}
				s.logger.Info("sync done", "db", dbName, "table", discoveredTable.Name, "dest_table", name, "rows", res.RowsCopied, "batches", res.Batches, "duration", res.Duration)
			}(tableName, w, channels[tableName])
		}
		wg.Wait()
		close(errs)

		for err := range errs {
			return err
		}
	}

	return nil
}

// func (s *Scheduler) LoadDatabasesWithTables() (sources []sourcecommon.DatabaseScheme) {
// 	// load databases
// 	var databases []*entity.AirbyteDatabase
// 	{
// 		switch foundDatabases, err := s.databaseService.GetList(); {
// 		case err != nil:
// 			log.Fatalf("load databases: %v", err)
// 		default:
// 			databases = foundDatabases.Databases
// 		}
// 	}

// 	for _, db := range databases {
// 		var (
// 			dbID     = db.ID
// 			isActive = true
// 		)

// 		// load tables
// 		var tableEntities []*entity.AirbyteTable
// 		{
// 			switch tableList, err := s.tableService.GetList(&airbytetableservice.GetAirByteTableListModel{
// 				DatabaseID: &dbID,
// 				IsActive:   &isActive,
// 			}); {
// 			case err != nil:
// 				log.Fatalf("load tables for db %s: %v", db.Name, err)
// 			default:
// 				tableEntities = tableList.Tables
// 			}
// 		}

// 		var source = sourcecommon.NewDatabaseScheme(db.URI, db.Name)
// 		for _, table := range tableEntities {
// 			var tableID = table.ID

// 			// load fields
// 			var fieldEntities []*entity.AirbyteField
// 			{
// 				switch fieldList, err := s.fieldService.GetList(&tableID); {
// 				case err != nil:
// 					log.Fatalf("load fields for table %s: %v", table.Name, err)
// 				default:
// 					fieldEntities = fieldList.Fields
// 				}
// 			}

// 			fields := make([]sourcecommon.Field, len(fieldEntities))
// 			for indx, field := range fieldEntities {
// 				fields[indx] = sourcecommon.Field{
// 					Name:            field.FieldName,
// 					DestinationType: field.DestinationType,
// 					IsChildTable:    field.IsChildTable != nil && *field.IsChildTable,
// 					Nullable:        field.Nullable,
// 					IsPrimary:       field.IsPrimary,
// 				}
// 			}

// 			source.Add(&sourcecommon.Table{
// 				Name:             table.Name,
// 				WriteMode:        core.WriteMode(table.WriteMode),
// 				PrimaryKey:       table.PrimaryKey,
// 				Fields:           fields,
// 				CreatedTimeField: table.CreatedTimeField,
// 				UpdatedTimeField: table.UpdatedTimeField,
// 				DeletedTimeField: table.DeletedTimeField,
// 			})
// 		}
// 		sources = append(sources, source)
// 	}

// 	return
// }
