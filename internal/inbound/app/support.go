package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/inbound"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/originup"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/replication"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/inbound/txn"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
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
