package tsqlx

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
)

type TracedDB struct {
	*sqlx.DB
	tracer      ITracer
	serviceName string
	verbose     bool
}

func NewTracedDB(
	db *sqlx.DB,
	tracer ITracer,
	serviceName string,
) *TracedDB {
	verbose := os.Getenv("TSQL_VERBOSE") == "true"
	return &TracedDB{
		tracer:      tracer,
		DB:          db,
		serviceName: serviceName,
		verbose:     verbose,
	}
}

func (db *TracedDB) logVerbose(query string, args ...interface{}) {
	if db.verbose {
		fmt.Printf("[TSQLX]\t %s %v\n", query, args)
	}
}

func (trDB *TracedDB) Get(
	ctx context.Context,
	dest interface{},
	query string,
	args ...interface{},
) error {
	go trDB.logVerbose(query, args...)
	start := time.Now()
	err := trDB.DB.Get(dest, query, args...)
	end := time.Now()
	if err != nil {
		trDB.tracer.TraceDependency(
			ctx,
			"",
			trDB.DriverName(),
			trDB.serviceName,
			"Get",
			false,
			start,
			end,
			map[string]string{
				"error": err.Error(),
				"query": query,
			},
		)
	} else {
		trDB.tracer.TraceDependency(
			ctx,
			"",
			trDB.DriverName(),
			trDB.serviceName,
			"Get",
			true,
			start,
			end,
			nil,
		)
	}
	return err
}

func (trDB *TracedDB) Select(
	ctx context.Context,
	dest interface{},
	query string,
	args ...interface{},
) error {
	go trDB.logVerbose(query, args...)
	start := time.Now()
	err := trDB.DB.Select(dest, query, args...)
	end := time.Now()
	if err != nil {
		trDB.tracer.TraceDependency(
			ctx,
			"",
			trDB.DriverName(),
			trDB.serviceName,
			"Get",
			false,
			start,
			end,
			map[string]string{
				"error": err.Error(),
				"query": query,
			},
		)
	} else {
		trDB.tracer.TraceDependency(
			ctx,
			"",
			trDB.DriverName(),
			trDB.serviceName,
			"Get",
			true,
			start,
			end,
			nil,
		)
	}
	return err
}

func (trDB *TracedDB) Exec(
	ctx context.Context,
	query string,
	args ...interface{},
) (sql.Result, error) {
	go trDB.logVerbose(query, args...)
	start := time.Now()
	res, err := trDB.DB.Exec(query, args...)
	end := time.Now()
	if err != nil {
		trDB.tracer.TraceDependency(
			ctx,
			"",
			trDB.DriverName(),
			trDB.serviceName,
			"Get",
			false,
			start,
			end,
			map[string]string{
				"error": err.Error(),
				"query": query,
			},
		)
	} else {
		trDB.tracer.TraceDependency(
			ctx,
			"",
			trDB.DriverName(),
			trDB.serviceName,
			"Get",
			true,
			start,
			end,
			nil,
		)
	}
	return res, err
}

func (trDB *TracedDB) NamedExec(
	ctx context.Context,
	query string,
	arg interface{},
) (sql.Result, error) {
	go trDB.logVerbose(query, arg)
	start := time.Now()
	res, err := trDB.DB.NamedExec(query, arg)
	end := time.Now()
	if err != nil {
		trDB.tracer.TraceDependency(
			ctx,
			"",
			trDB.DriverName(),
			trDB.serviceName,
			"Get",
			false,
			start,
			end,
			map[string]string{
				"error": err.Error(),
				"query": query,
			},
		)
	} else {
		trDB.tracer.TraceDependency(
			ctx,
			"",
			trDB.DriverName(),
			trDB.serviceName,
			"Get",
			true,
			start,
			end,
			nil,
		)
	}
	return res, err
}

func (db *TracedDB) Beginx() (*TracedTx, error) {

	tx, err := db.DB.Beginx()
	return &TracedTx{
		Tx:          tx,
		tracer:      db.tracer,
		serviceName: db.serviceName,
	}, err
}

func (db *TracedDB) MustBegin() *TracedTx {
	tx := db.DB.MustBegin()
	return &TracedTx{
		Tx:          tx,
		tracer:      db.tracer,
		serviceName: db.serviceName,
	}
}
