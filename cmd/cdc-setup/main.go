// cdc-setup is an explicit local administrative step, not an API startup side effect.
package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/su944/distributed-job-engine/internal/config"
	"github.com/su944/distributed-job-engine/internal/dispatch"
	"github.com/su944/distributed-job-engine/internal/storage"
	"github.com/su944/distributed-job-engine/migrations"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if err := run(); err != nil {
		slog.Error("CDC setup failed; inspect service status and local configuration")
		os.Exit(1)
	}
}
func run() error {
	if os.Getenv("CDC_PASSWORD") == "" {
		return errors.New("CDC_PASSWORD required")
	}
	c, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	kc, err := dispatch.LoadConfig(os.Getenv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, err := storage.Open(ctx, c)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := migrations.Apply(ctx, db); err != nil {
		return err
	}
	slog.Info("CDC setup migration ready")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(84217002)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DO $$ BEGIN IF NOT EXISTS(SELECT FROM pg_roles WHERE rolname='jobengine_cdc') THEN CREATE ROLE jobengine_cdc LOGIN REPLICATION; END IF; END $$`); err != nil {
		return err
	}
	// PostgreSQL DDL cannot bind a password parameter. Quote it on the server via
	// format(%L); never interpolate raw environment values or log the result.
	var alter string
	if err := tx.QueryRowContext(ctx, `SELECT format('ALTER ROLE jobengine_cdc WITH LOGIN REPLICATION PASSWORD %L',$1::text)`, os.Getenv("CDC_PASSWORD")).Scan(&alter); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, alter); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `GRANT CONNECT ON DATABASE jobengine TO jobengine_cdc; GRANT USAGE ON SCHEMA public TO jobengine_cdc; GRANT SELECT ON public.outbox_events TO jobengine_cdc`); err != nil {
		return err
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT FROM pg_publication WHERE pubname='jobs_outbox')`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err := tx.ExecContext(ctx, `CREATE PUBLICATION jobs_outbox FOR TABLE public.outbox_events WITH(publish='insert')`); err != nil {
			return err
		}
	}
	var correct bool
	if err := tx.QueryRowContext(ctx, `SELECT pubinsert AND NOT pubupdate AND NOT pubdelete AND NOT pubtruncate AND NOT puballtables
        AND (SELECT count(*)=1 FROM pg_publication_tables WHERE pubname='jobs_outbox')
        AND EXISTS(SELECT FROM pg_publication_tables WHERE pubname='jobs_outbox' AND schemaname='public' AND tablename='outbox_events') FROM pg_publication WHERE pubname='jobs_outbox'`).Scan(&correct); err != nil || !correct {
		return errors.New("publication configuration differs from reviewed INSERT-only scope")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	slog.Info("CDC setup replication credentials and publication ready")
	client, err := kgo.NewClient(kgo.SeedBrokers(kc.Brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	results, err := admin.CreateTopics(ctx, 3, 1, map[string]*string{"cleanup.policy": kadm.StringPtr("delete"), "retention.ms": kadm.StringPtr("604800000"), "max.message.bytes": kadm.StringPtr("1048576")}, dispatch.Topic)
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Err != nil && !errors.Is(result.Err, kerr.TopicAlreadyExists) {
			return result.Err
		}
	}
	slog.Info("CDC setup notification topic ready")
	// The internal heartbeat topic is not transformed by the outbox SMT.
	results, err = admin.CreateTopics(ctx, 1, 1, nil, "__debezium-heartbeat.jobengine")
	if err != nil {
		return err
	}
	for _, r := range results {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return r.Err
		}
	}
	data, err := os.ReadFile("deploy/debezium-outbox.json")
	if err != nil {
		return err
	}
	url := os.Getenv("CONNECT_URL")
	if url == "" {
		url = "http://127.0.0.1:18083"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url+"/connectors/job-engine-outbox/config", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		slog.Error("connector configuration rejected", "http_status", response.StatusCode)
		return errors.New("connector configuration rejected")
	}
	slog.Info("CDC configured", "connector", "job-engine-outbox", "topic", dispatch.Topic)
	return nil
}
