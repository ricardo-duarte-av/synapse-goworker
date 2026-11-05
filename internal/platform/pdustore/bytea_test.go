package pdustore

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// Synapse runs SET bytea_output TO escape on its connections, and through
// pgcat a server connection keeps it: every bytea read here is hex-encoded
// in SQL so that it decodes either way. Against a throwaway PostgreSQL:
//
//	PDUSTORE_TEST_DSN="host=127.0.0.1 port=55432 user=synapse password=test dbname=synapse"
func TestByteaUnderEscapeOutput(t *testing.T) {
	dsn := os.Getenv("PDUSTORE_TEST_DSN")
	if dsn == "" {
		t.Skip("PDUSTORE_TEST_DSN not set")
	}
	if strings.Contains(dsn, "/var/sockets") {
		t.Fatal("PDUSTORE_TEST_DSN must be a throwaway database")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	ctx := context.Background()
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, "SET bytea_output TO escape"); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := c.QueryRow(ctx, `SELECT '{"a":1}'::bytea`).Scan(&raw); err == nil {
		t.Fatal("pgx decoded escape output: the workaround may no longer be needed")
	}
	var hexed string
	if err := c.QueryRow(ctx, `SELECT encode('{"a":1}'::bytea, 'hex')`).Scan(&hexed); err != nil || hexed != "7b2261223a317d" {
		t.Fatalf("%q %v", hexed, err)
	}
}
