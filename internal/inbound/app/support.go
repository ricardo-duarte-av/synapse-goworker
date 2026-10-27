package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/dbrole"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/inbound"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/originup"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/pdustore"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/store"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/synapsecfg"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/txn"
)

// openOriginUp builds the resetter for origins in backoff, on the write
// role and homeserver.yaml's replication bus.
func openOriginUp(ctx context.Context, instance string, hs *synapsecfg.Config, wdb *inbound.DB, log zerolog.Logger) (*originup.Bus, *originup.Resetter, error) {
	if !hs.Redis.Enabled {
		return nil, nil, errors.New("homeserver.yaml has no redis, so nothing can be told the origin is up")
	}
	bus := originup.NewBus(hs.Redis, hs.ServerName, log)
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := bus.Ping(pctx); err != nil {
		bus.Close()
		return nil, nil, fmt.Errorf("replication bus: %w", err)
	}
	r := originup.New(originup.Options{Store: wdb, Publisher: bus, Instance: instance, Log: log})
	log.Info().Str("instance", instance).Msg("Origins in backoff are served here and marked as up")
	return bus, r, nil
}

// openDatabase connects as the read-only role, making sure first that the role
// exists and has exactly what it needs.
//
// On every start: connect as the role and check its grants. If that fails --
// no such role, a wrong password, a missing grant -- create or repair the role
// with homeserver.yaml's credentials and try once more. Those credentials are
// used for nothing else, and the connection made with them is closed before
// serving starts.
func openDatabase(ctx context.Context, cfg Settings, hs *synapsecfg.Config, log zerolog.Logger) (*store.Store, store.Config, error) {
	role := cfg.ReadRole
	password := dbrole.Password(hs.Database.Password, role)

	dsn := cfg.DSN
	if dsn == "" {
		if !hs.Database.Present {
			return nil, store.Config{}, errors.New("no database: set database.dsn, or give homeserver.yaml a database block")
		}
		serving := hs.Database
		serving.User, serving.Password = role, password
		dsn = serving.DSN()
	}

	maxConns := cfg.ReadMaxConns
	if maxConns == 0 {
		maxConns = 4
	}
	connect := cfg.ConnectTimeout
	if connect == 0 {
		connect = 10 * time.Second
	}
	storeCfg := store.Config{
		DSN:            dsn,
		User:           role,
		Password:       password,
		MaxConns:       maxConns,
		ConnectTimeout: connect,
		QueryTimeout:   5 * time.Second,
	}

	db, problem := openChecked(ctx, storeCfg, connect)
	if problem == nil {
		log.Info().Str("role", role).Msg("Connected to the database as the read-only role")
		return db, storeCfg, nil
	}
	if !cfg.ManageRoles {
		return nil, store.Config{}, fmt.Errorf("database role %s: %w (database.manage_roles is false; create it by hand)", role, problem)
	}
	if !hs.Database.Present {
		return nil, store.Config{}, fmt.Errorf("database role %s: %w, and homeserver.yaml has no database block to create it with", role, problem)
	}

	log.Warn().Err(problem).Str("role", role).Str("admin", hs.Database.Redacted()).
		Msg("Read-only role is missing or incomplete; creating or repairing it with homeserver.yaml's credentials")
	ensureCtx, cancel := context.WithTimeout(ctx, connect+20*time.Second)
	err := dbrole.Ensure(ensureCtx, hs.Database.DSN(), dbrole.Spec{Role: role, Password: password, Tables: roleTables()})
	cancel()
	if err != nil {
		return nil, store.Config{}, fmt.Errorf("database role %s: %w; and could not fix it: %w", role, problem, err)
	}

	db, err = openChecked(ctx, storeCfg, connect)
	if err != nil {
		// The role is right now, so the connection itself is the problem.
		// The usual cause is a pooler that only knows the users in its own
		// config.
		return nil, store.Config{}, fmt.Errorf("database role %s was created, but connecting as it still fails: %w "+
			"(if database.dsn goes through pgcat or pgbouncer, add the role there, or point database.dsn "+
			"at PostgreSQL directly, e.g. host=/var/sockets dbname=%s)", role, err, hs.Database.DBName)
	}
	log.Info().Str("role", role).Msg("Created or repaired the read-only role, and connected as it")
	return db, storeCfg, nil
}

// persister sends events to the room's event persister.
type persister struct {
	c       *replication.Client
	writers synapsecfg.Writers
}

func (p persister) SendEvents(ctx context.Context, roomID string, entries []json.RawMessage, backfilled bool) error {
	_, err := p.c.SendEvents(ctx, p.writers.EventsWriter(roomID), roomID, entries, backfilled)
	return err
}

func (p persister) ResyncDevices(ctx context.Context, userID string) error {
	w := synapsecfg.MainProcessInstanceName
	if len(p.writers.DeviceLists) > 0 {
		w = p.writers.DeviceLists[0]
	}
	return p.c.MultiUserDeviceResync(ctx, w, []string{userID})
}

// frontDoor adapts write mode's front door to txn.PDUServer.
type frontDoor struct{ f *inbound.FrontDoor }

func (f frontDoor) PlanPDUs(ctx context.Context, origin string, pdus []json.RawMessage, at time.Time) (txn.PDURun, string) {
	p, why := f.f.PlanPDUs(ctx, origin, pdus, at)
	if why != "" {
		return nil, why
	}
	return p, ""
}

// openWriteDatabase connects as the write role, creating or repairing it
// first with homeserver.yaml's credentials: every start, since the grants
// must be exactly inbound.Grants and Ensure makes them so.
func openWriteDatabase(ctx context.Context, cfg Settings, hs *synapsecfg.Config, log zerolog.Logger) (*inbound.DB, error) {
	role := cfg.WriteRole
	if !hs.Database.Present {
		return nil, errors.New("write mode: homeserver.yaml has no database block")
	}
	password := dbrole.Password(hs.Database.Password, role)
	ensureCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := dbrole.Ensure(ensureCtx, hs.Database.DSN(), dbrole.Spec{
		Role: role, Password: password, Writes: inbound.Grants, Sequences: inbound.Sequences,
	})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("write role %s: %w", role, err)
	}
	serving := hs.Database
	serving.User, serving.Password = role, password
	dsn := serving.DSN()
	if cfg.DSN != "" {
		// The read-only role's direct connection, as the write role.
		c, err := pgx.ParseConfig(cfg.DSN)
		if err != nil {
			return nil, err
		}
		dsn = fmt.Sprintf("host=%s port=%d dbname=%s user=%s", c.Host, c.Port, c.Database, role)
		if password != "" {
			dsn += " password=" + password
		}
	}
	db, err := inbound.Open(ctx, dsn, cfg.InstanceName)
	if err != nil {
		return nil, fmt.Errorf("write role %s: %w", role, err)
	}
	log.Info().Str("role", role).Msg("Connected to the database as the write role")
	return db, nil
}

// roleTables is every table the read-only role is granted: those the EDU path
// reads (package store) and those the PDU checker reads (package pdustore).
func roleTables() []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range append(append([]string{}, store.Tables...), pdustore.Tables...) {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// openChecked connects and verifies the role's grants, returning why not.
func openChecked(ctx context.Context, cfg store.Config, connect time.Duration) (*store.Store, error) {
	openCtx, cancel := context.WithTimeout(ctx, connect+5*time.Second)
	defer cancel()
	db, err := store.Open(openCtx, cfg)
	if err != nil {
		return nil, err
	}
	missing, err := db.MissingPrivileges(openCtx, roleTables())
	if err != nil {
		db.Close()
		return nil, err
	}
	if len(missing) > 0 {
		db.Close()
		return nil, fmt.Errorf("role lacks %s", strings.Join(missing, ", "))
	}
	return db, nil
}
