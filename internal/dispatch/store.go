package dispatch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
)

type Outcome string

const (
	Received    Outcome = "received"
	Duplicate   Outcome = "duplicate"
	Quarantined Outcome = "quarantined"
)

type Store interface {
	Handle(context.Context, Record) (Outcome, error)
}
type SQLStore struct {
	db            *sql.DB
	source, group string
}

func NewSQLStore(db *sql.DB, source, group string) *SQLStore { return &SQLStore{db, source, group} }

func (s *SQLStore) Ready(ctx context.Context) error {
	var version int
	return s.db.QueryRowContext(ctx, `SELECT version FROM schema_migrations WHERE version=2
        AND NOT EXISTS(SELECT event_id FROM dispatch_events WHERE false)
        AND NOT EXISTS(SELECT source FROM dispatch_quarantine WHERE false)`).Scan(&version)
}

func (s *SQLStore) Handle(ctx context.Context, r Record) (Outcome, error) {
	if r.Partition < 0 || r.Offset < 0 || len(r.Topic) == 0 || len(r.Topic) > 249 {
		return "", errors.New("invalid Kafka coordinates")
	}
	e, validation := Decode(r)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin receipt: %w", err)
	}
	defer tx.Rollback()
	outcome := Received
	reason := ""
	if validation != nil {
		reason = validation.(*InvalidEvent).Code
	} else {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE id=$1)`, e.JobID).Scan(&exists); err != nil {
			return "", err
		}
		if !exists {
			reason = "unknown_job"
		} else {
			hash := e.digest()
			result, err := tx.ExecContext(ctx, `INSERT INTO dispatch_events(event_id,job_id,schema_version,occurred_at,envelope_hash) VALUES($1,$2,$3,$4,$5) ON CONFLICT(event_id) DO NOTHING`, e.EventID, e.JobID, e.SchemaVersion, e.OccurredAt, hash[:])
			if err != nil {
				return "", err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return "", err
			}
			if n == 0 {
				var old []byte
				if err := tx.QueryRowContext(ctx, `SELECT envelope_hash FROM dispatch_events WHERE event_id=$1`, e.EventID).Scan(&old); err != nil {
					return "", err
				}
				if !bytes.Equal(old, hash[:]) {
					reason = "event_id_conflict"
				} else {
					outcome = Duplicate
				}
			}
		}
	}
	if reason != "" {
		if err := s.quarantine(ctx, tx, r, reason); err != nil {
			return "", err
		}
		outcome = Quarantined
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("receipt commit uncertain: %w", err)
	}
	return outcome, nil
}

func (s *SQLStore) quarantine(ctx context.Context, tx *sql.Tx, r Record, reason string) error {
	kh, vh := sha256.Sum256(r.Key), sha256.Sum256(r.Value)
	_, err := tx.ExecContext(ctx, `INSERT INTO dispatch_quarantine(source,consumer_group,topic,partition,record_offset,reason,key_prefix,value_prefix,key_hash,value_hash,key_bytes,value_bytes)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING`, s.source, s.group, r.Topic, r.Partition, r.Offset, reason, prefix(r.Key, 1024), prefix(r.Value, MaxEventBytes), kh[:], vh[:], len(r.Key), len(r.Value))
	if err != nil {
		return err
	}
	var oldKey, oldValue []byte
	if err := tx.QueryRowContext(ctx, `SELECT key_hash,value_hash FROM dispatch_quarantine WHERE source=$1 AND consumer_group=$2 AND topic=$3 AND partition=$4 AND record_offset=$5`, s.source, s.group, r.Topic, r.Partition, r.Offset).Scan(&oldKey, &oldValue); err != nil {
		return err
	}
	if !bytes.Equal(kh[:], oldKey) || !bytes.Equal(vh[:], oldValue) {
		return errors.New("quarantine coordinates reused; rotate source epoch after cluster/topic replacement")
	}
	return nil
}

func prefix(b []byte, n int) []byte {
	if len(b) > n {
		b = b[:n]
	}
	if b == nil {
		return []byte{}
	}
	return b
}
