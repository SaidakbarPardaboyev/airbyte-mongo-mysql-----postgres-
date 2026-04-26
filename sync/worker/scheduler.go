package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	// "airbyte-service/core"
	// "airbyte-service/database/mysql/entity"
	"airbyte-service/core"
	"airbyte-service/plugins"

	// airbytedbservice "airbyte-service/service/airbyte-database"
	// airbytefieldservice "airbyte-service/service/airbyte-field"
	// airbytetableservice "airbyte-service/service/airbyte-table"
	// synchistoryservice "airbyte-service/service/sync-history"
	sourcecommon "airbyte-service/sync/sources/common"
)

type Scheduler struct {
	// databaseService    airbytedbservice.Service
	// tableService       airbytetableservice.Service
	// fieldService       airbytefieldservice.Service
	// syncHistoryService synchistoryservice.Service

	interval    time.Duration
	destination *pgxpool.Pool
	mu          sync.Mutex
	logger      *slog.Logger
	running     bool

	lastSyncEndTimeFilter       time.Time
	currentSyncEndingTimeFilter time.Time
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.trigger(ctx)

	if s.interval == 0 {
		return fmt.Errorf("interval not given")
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.trigger(ctx)
		}
	}
}

func (s *Scheduler) trigger(ctx context.Context) {
	var err error
	var startDate = plugins.GetNow()
	s.mu.Lock()
	defer func() {
		s.running = false
		s.mu.Unlock()
	}()

	s.logger.Info("sync started", "started_at", startDate.Format(time.RFC3339))

	// check there is a running job
	{
		if s.running {
			s.logger.Warn("scheduler: previous sync still running, skipping tick")
			return
		}
		s.running = true
	}

	// add ending filter for loading data
	s.currentSyncEndingTimeFilter = plugins.GetNow().Add(-time.Minute)

	// // create a sync history
	// var history *entity.SyncHistory
	// {
	// 	switch createdHistory, failure := s.syncHistoryService.Create(&synchistoryservice.CreateSyncHistoryModel{
	// 		Status:         core.SyncStatusRunning,
	// 		StartDate:      startDate,
	// 		IsManualRunned: false,
	// 		EndDateFilter:  s.currentSyncEndingTimeFilter,
	// 	}); {
	// 	case failure != nil:
	// 		err = failure
	// 		return
	// 	default:
	// 		history = createdHistory.History
	// 	}
	// }

	// load databases with tables and fields
	var sourcesFromDatabase []sourcecommon.DatabaseScheme
	{
		// sourcesFromDatabase = s.LoadDatabasesWithTables()
		sourcesFromDatabase = buildSources()
	}

	// run job
	{
		err = s.runSync(ctx, sourcesFromDatabase)

		// // calculate ending time
		// history.EndDate = plugins.GetNowPtr()

		if err != nil {
			// history.Status = core.SyncStatusFailed
			// history.Error = plugins.Ptr(err.Error())

			s.logger.Error("sync failed", "error", err)
		} else {
			s.lastSyncEndTimeFilter = s.currentSyncEndingTimeFilter

			// history.Status = core.SyncStatusSucceeded
			// history.EndDateFilter = s.lastSyncEndTimeFilter

			s.logger.Info("sync succeeded")
		}
	}

	// // update history as runned
	// {
	// 	switch updatedHistory, failure := s.syncHistoryService.Update(&synchistoryservice.UpdateSyncHistoryModel{
	// 		ID:            history.ID,
	// 		Status:        history.Status,
	// 		EndDate:       history.EndDate,
	// 		Error:         history.Error,
	// 		EndDateFilter: history.EndDateFilter,
	// 	}); {
	// 	case failure != nil:
	// 		err = failure
	// 		s.logger.Error("sync history saving failed", "error", err)
	// 		return
	// 	case updatedHistory.NotFound:
	// 		s.logger.Error("history not found on finishing a job:", "history id", history.ID)
	// 	}
	// }
}

func New(
	interval time.Duration,
	destination *pgxpool.Pool,
	// databaseService airbytedbservice.Service,
	// tableService airbytetableservice.Service,
	// fieldService airbytefieldservice.Service,
	// syncHistoryService synchistoryservice.Service,
	logger *slog.Logger,
) (*Scheduler, error) {
	// take last ending date filter
	var lastSyncEndTimeFilter time.Time
	// {
	// 	switch foundHistory, failure := syncHistoryService.GetLastHistory(); {
	// 	case failure != nil:
	// 		return nil, failure
	// 	case !foundHistory.NotFound:
	// 		lastSyncEndTimeFilter = foundHistory.History.EndDateFilter
	// 	}
	// }

	return &Scheduler{
		// databaseService:    databaseService,
		// tableService:       tableService,
		// fieldService:       fieldService,
		// syncHistoryService: syncHistoryService,

		interval:              interval,
		destination:           destination,
		lastSyncEndTimeFilter: lastSyncEndTimeFilter,
		logger:                logger,
		mu:                    sync.Mutex{},
	}, nil
}

func buildSources() []sourcecommon.DatabaseScheme {
	database := sourcecommon.NewDatabaseScheme(core.MongoConnectionString, core.MongoDatabaseName)
	table := &sourcecommon.Table{
		Name:             core.MongoTableName,
		WriteMode:        core.WriteModeUpsert,
		CreatedTimeField: "date",
		UpdatedTimeField: "updated_at",
		DeletedTimeField: "deleted_at",
		Fields: []sourcecommon.Field{
			{Name: "id", DestinationType: "VARCHAR", IsPrimary: true},
			{Name: "account.id", DestinationType: "VARCHAR"},
			{Name: "account.name", DestinationType: "VARCHAR"},
			// {Name: "account.username", DestinationType: "VARCHAR"},
			{Name: "approved_at", DestinationType: "TIMESTAMPTZ"},
			// {Name: "branch.id", DestinationType: "VARCHAR"},
			// {Name: "branch.name", DestinationType: "VARCHAR"},
			{Name: "cash_box.id", DestinationType: "VARCHAR"},
			{Name: "cash_box.name", DestinationType: "VARCHAR"},
			// {Name: "collected_at", DestinationType: "TIMESTAMPTZ"},
			{Name: "contractor.id", DestinationType: "VARCHAR"},
			// {Name: "contractor.inn", DestinationType: "VARCHAR"},
			{Name: "contractor.name", DestinationType: "VARCHAR"},
			{Name: "created_at", DestinationType: "TIMESTAMPTZ"},
			{Name: "date", DestinationType: "TIMESTAMPTZ"},
			{Name: "deleted_at", DestinationType: "TIMESTAMPTZ"},
			// {Name: "delivery_info", DestinationType: "VARCHAR"},
			{Name: "employee.id", DestinationType: "BIGINT"},
			{Name: "employee.name", DestinationType: "VARCHAR"},
			// {Name: "handed_over_at", DestinationType: "TIMESTAMPTZ"},
			{Name: "is_approved", DestinationType: "BOOLEAN"},
			// {Name: "is_collected", DestinationType: "BOOLEAN"},
			{Name: "is_deleted", DestinationType: "BOOLEAN"},
			// {Name: "is_fiscalized", DestinationType: "BOOLEAN"},
			// {Name: "is_for_debt", DestinationType: "BOOLEAN"},
			// {Name: "is_handed_over", DestinationType: "BOOLEAN"},
			// {Name: "is_reviewed", DestinationType: "BOOLEAN"},
			{Name: "number", DestinationType: "VARCHAR"},
			// {Name: "location", DestinationType: "VARCHAR"},
			{Name: "organization.id", DestinationType: "VARCHAR"},
			{Name: "organization.name", DestinationType: "VARCHAR"},
			{Name: "payment.cash_box_states", DestinationType: "JSONB"},
			{Name: "payment.date", DestinationType: "TIMESTAMPTZ"},
			{Name: "payment.debt_states", DestinationType: "JSONB"},
			{Name: "payment.id", DestinationType: "VARCHAR"},
			// {Name: "payment.notes", DestinationType: "VARCHAR"},
			{Name: "percent_discount", DestinationType: "DOUBLE PRECISION"},
			// {Name: "reviewed_at", DestinationType: "TIMESTAMPTZ"},
			{Name: "status", DestinationType: "INTEGER"},
			// {Name: "tags", DestinationType: "JSONB"},
			{Name: "updated_at", DestinationType: "TIMESTAMPTZ"},
			{Name: "exact_discounts", DestinationType: "JSONB"},
			// {Name: "exact_discounts.amount", DestinationType: "DOUBLE PRECISION"},
			// {Name: "exact_discounts.currency.id", DestinationType: "BIGINT"},
			// {Name: "exact_discounts.currency.is_national", DestinationType: "BOOLEAN"},
			// {Name: "exact_discounts.currency.name", DestinationType: "VARCHAR"},
			{Name: "net_price", DestinationType: "JSONB"},
			// {Name: "net_price.amount", DestinationType: "DOUBLE PRECISION"},
			// {Name: "net_price.currency.id", DestinationType: "BIGINT"},
			// {Name: "net_price.currency.is_national", DestinationType: "BOOLEAN"},
			// {Name: "net_price.currency.name", DestinationType: "VARCHAR"},

			{Name: "items", DestinationType: "JSONB", IsChildTable: true},
			{Name: "items.created_at", DestinationType: "TIMESTAMPTZ"},
			{Name: "items.deleted_at", DestinationType: "TIMESTAMPTZ"},
			{Name: "items.discount.amount", DestinationType: "DOUBLE PRECISION"},
			{Name: "items.discount.type", DestinationType: "INTEGER"},
			{Name: "items.discount.value", DestinationType: "DOUBLE PRECISION"},
			{Name: "items.id", DestinationType: "VARCHAR", IsPrimary: true},
			{Name: "items.is_deleted", DestinationType: "BOOLEAN"},
			{Name: "items.net_price.amount", DestinationType: "DOUBLE PRECISION"},
			{Name: "items.net_price.currency.id", DestinationType: "BIGINT"},
			// {Name: "items.net_price.currency.is_national", DestinationType: "BOOLEAN"},
			{Name: "items.net_price.currency.name", DestinationType: "VARCHAR"},
			{Name: "items.original_price.amount", DestinationType: "DOUBLE PRECISION"},
			{Name: "items.original_price.currency.id", DestinationType: "BIGINT"},
			// {Name: "items.original_price.currency.is_national", DestinationType: "BOOLEAN"},
			{Name: "items.original_price.currency.name", DestinationType: "VARCHAR"},
			{Name: "items.price.amount", DestinationType: "DOUBLE PRECISION"},
			{Name: "items.price.currency.id", DestinationType: "BIGINT"},
			// {Name: "items.price.currency.is_national", DestinationType: "BOOLEAN"},
			{Name: "items.price.currency.name", DestinationType: "VARCHAR"},
			{Name: "items.quantity", DestinationType: "DOUBLE PRECISION"},
			{Name: "items.updated_at", DestinationType: "TIMESTAMPTZ"},
			{Name: "items.warehouse_item.id", DestinationType: "VARCHAR"},
			// {Name: "items.warehouse_item.marks", DestinationType: "JSONB"},
			{Name: "items.warehouse_item.name", DestinationType: "VARCHAR"},
			{Name: "items.warehouse_item.warehouse.id", DestinationType: "INTEGER"},
			{Name: "items.warehouse_item.warehouse.name", DestinationType: "VARCHAR"},
			// {Name: "items.warehouse_item.warehouse_item_use.after_quantity", DestinationType: "DOUBLE PRECISION"},
			// {Name: "items.warehouse_item.warehouse_item_use.before_quantity", DestinationType: "DOUBLE PRECISION"},
			// {Name: "items.warehouse_item.warehouse_item_use.id", DestinationType: "BIGINT"},
		},
	}
	database.Add(table)
	return []sourcecommon.DatabaseScheme{database}
}
