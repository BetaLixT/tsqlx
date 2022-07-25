package tsqlx

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
)

type TracedTx struct {
  *sqlx.Tx
	tracer      ITracer
	serviceName string
}


func (tx *TracedTx) Get(
	ctx context.Context,
	dest interface{},
	query string,
	args ...interface{},
) error {
	start := time.Now()
	err := tx.Tx.Get(dest, query, args...)
	end := time.Now()
	if err != nil {
		tx.tracer.TraceDependency(
			ctx,
			"",
			tx.DriverName(),
			tx.serviceName,
			"Get",
			false,
			start,
			end,
			map[string]string {
				"error": err.Error(),
				"query": query,
			},
		)
	} else {
		tx.tracer.TraceDependency(
			ctx,
			"",
			tx.DriverName(),
			tx.serviceName,
			"Get",
			true,
			start,
			end,
			nil,
		)
	}
	return err
}

func (tx *TracedTx) Select(
	ctx context.Context,
	dest interface{},
	query string,
	args ...interface{},
) error {
	start := time.Now()
	err := tx.Tx.Select(dest, query, args...)
	end := time.Now()
	if err != nil {
		tx.tracer.TraceDependency(
			ctx,
			"",
			tx.DriverName(),
			tx.serviceName,
			"Get",
			false,
			start,
			end,
			map[string]string {
				"error": err.Error(),
				"query": query,
			},
		)
	} else {
		tx.tracer.TraceDependency(
		  ctx,
			"",
			tx.DriverName(),
			tx.serviceName,
			"Get",
			true,
			start,
			end,
			nil,
		)
	}
	return err
}

func (tx *TracedTx) Exec(
	ctx context.Context,
	query string,
	args ...interface{},
) (sql.Result, error) {
	start := time.Now()
	res, err := tx.Tx.Exec(query, args...)
	end := time.Now()
	if err != nil {
		tx.tracer.TraceDependency(
			ctx,
			"",
			tx.DriverName(),
			tx.serviceName,
			"Get",
			false,
			start,
			end,
			map[string]string {
				"error": err.Error(),
				"query": query,
			},
		)
	} else {
		tx.tracer.TraceDependency(
			ctx,
			"",
			tx.DriverName(),
			tx.serviceName,
			"Get",
			true,
			start,
			end,
			nil,
		)
	}
	return res, err
}

func (tx *TracedTx) NamedExec(
	ctx context.Context,
	query string,
	arg interface{},
) (sql.Result, error) {

	start := time.Now()
	res, err := tx.Tx.NamedExec(query, arg)
	end := time.Now()
	if err != nil {
		tx.tracer.TraceDependency(
			ctx,
			"",
			tx.DriverName(),
			tx.serviceName,
			"Get",
			false,
			start,
			end,
			map[string]string {
				"error": err.Error(),
				"query": query,
			},
		)
	} else {
		tx.tracer.TraceDependency(
			ctx,
			"",
			tx.DriverName(),
			tx.serviceName,
			"Get",
			true,
			start,
			end,
			nil,
		)
	}
	return res, err
}
