package goqueue

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDDLUsesConfiguredTableName(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)

	q, err := New(db, Config{QueueName: "test", TableName: "order"})
	require.NoError(t, err)

	ddl := q.DDL()
	assert.Contains(t, ddl, "CREATE TABLE IF NOT EXISTS `order`")
}

func TestAutoMigrateExecutesDDL(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	q, err := New(db, Config{QueueName: "test"})
	require.NoError(t, err)

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, q.AutoMigrate(context.Background()))
	assert.NoError(t, mock.ExpectationsWereMet())
}
