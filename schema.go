package goqueue

import (
	"context"
	"fmt"
)

// DDL returns the CREATE TABLE statement for the configured table.
func (q *Queue) DDL() string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n"+
		"  id              BIGINT        NOT NULL AUTO_INCREMENT,\n"+
		"  queue_name      VARCHAR(128)  NOT NULL,\n"+
		"  idempotency_key VARCHAR(255)  NULL,\n"+
		"  payload         JSON          NOT NULL,\n"+
		"  state           VARCHAR(16)   NOT NULL DEFAULT 'pending',\n"+
		"  priority        INT           NOT NULL DEFAULT 0,\n"+
		"  attempts        INT           NOT NULL DEFAULT 0,\n"+
		"  max_attempts    INT           NOT NULL DEFAULT 8,\n"+
		"  next_attempt_at TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"+
		"  claimed_by      VARCHAR(128)  NULL,\n"+
		"  claimed_at      TIMESTAMP     NULL,\n"+
		"  claimed_until   TIMESTAMP     NULL,\n"+
		"  last_error      TEXT          NULL,\n"+
		"  created_at      TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"+
		"  updated_at      TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"+
		"  completed_at    TIMESTAMP     NULL,\n"+
		"  PRIMARY KEY (id),\n"+
		"  UNIQUE KEY uk_idempotency (queue_name, idempotency_key),\n"+
		"  KEY ix_claimable (queue_name, state, next_attempt_at, priority, id),\n"+
		"  KEY ix_lease_expiry (queue_name, state, claimed_until)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
		q.cfg.TableName)
}

// AutoMigrate creates the configured table if it does not already exist.
func (q *Queue) AutoMigrate(ctx context.Context) error {
	_, err := q.db.ExecContext(ctx, q.DDL())
	if err != nil {
		return fmt.Errorf("goqueue: AutoMigrate: %w", err)
	}
	return nil
}
