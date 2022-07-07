
package tsqlx

import (
	"database/sql"
	"time"
	"github.com/jmoiron/sqlx"
)

type TracedDB struct {
	*sqlx.DB
	tracer      ITracer
	serviceName string
}

func NewTracedDB(
	db *sqlx.DB,
	tracer ITracer,
	serviceName string,
) *TracedDB {

	return &TracedDB{
		tracer:      tracer,
		DB:          db,
		serviceName: serviceName,
	}
}

func (trDB *TracedDB) Get(
	dest interface{},
	query string,
	args ...interface{},
) error {
	start := time.Now()
	err := trDB.DB.Get(dest, query, args...)
	end := time.Now()
	if err != nil {
		trDB.tracer.TraceDependency(
			"",
			trDB.DriverName(),
			trDB.serviceName,
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
		trDB.tracer.TraceDependency(
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
	dest interface{},
	query string,
	args ...interface{},
) error {

	start := time.Now()
	err := trDB.DB.Select(dest, query, args...)
	end := time.Now()
	if err != nil {
		trDB.tracer.TraceDependency(
			"",
			trDB.DriverName(),
			trDB.serviceName,
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
		trDB.tracer.TraceDependency(
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
	query string,
	args ...interface{},
) (sql.Result, error) {

	start := time.Now()
	res, err := trDB.DB.Exec(query, args...)
	end := time.Now()
	if err != nil {
		trDB.tracer.TraceDependency(
			"",
			trDB.DriverName(),
			trDB.serviceName,
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
		trDB.tracer.TraceDependency(
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
	query string,
	arg interface{},
) (sql.Result, error) {

	start := time.Now()
	res, err := trDB.DB.NamedExec(query, arg)
	end := time.Now()
	if err != nil {
		trDB.tracer.TraceDependency(
			"",
			trDB.DriverName(),
			trDB.serviceName,
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
		trDB.tracer.TraceDependency(
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
