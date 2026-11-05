package pushwriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/sync/pushrules"
)

// DB is what the push-rules writer reads and writes in Synapse's database,
// through its own pool, in Exec mode (pgcat).
type DB struct {
	pool *pgxpool.Pool
	// ruleIDs and enableIDs are IdGenerator(push_rules, id) and
	// IdGenerator(push_rules_enable, id): in memory, from MAX(id) at start,
	// as only the writer inserts.
	ruleIDs, enableIDs *idGen
}

type idGen struct {
	mu   sync.Mutex
	next int64
}

func (g *idGen) get() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return g.next
}

// Open connects and loads the two ID generators.
func Open(ctx context.Context, dsn string, maxConns int32, connectTimeout time.Duration) (*DB, error) {
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pushwriter: parse dsn: %w", err)
	}
	pcfg.MaxConns = max(maxConns, 2)
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	if connectTimeout > 0 {
		pcfg.ConnConfig.ConnectTimeout = connectTimeout
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("pushwriter: connect: %w", err)
	}
	d, err := NewDB(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return d, nil
}

// NewDB uses pool, loading the ID generators (_load_current_id: MAX, or 1).
func NewDB(ctx context.Context, pool *pgxpool.Pool) (*DB, error) {
	load := func(table string) (*idGen, error) {
		var v *int64
		if err := pool.QueryRow(ctx, `SELECT MAX(id) FROM `+table).Scan(&v); err != nil {
			return nil, fmt.Errorf("pushwriter: %s ids: %w", table, err)
		}
		g := &idGen{next: 1}
		if v != nil && *v > 1 {
			g.next = *v
		}
		return g, nil
	}
	d := &DB{pool: pool}
	var err error
	if d.ruleIDs, err = load("push_rules"); err != nil {
		return nil, err
	}
	if d.enableIDs, err = load("push_rules_enable"); err != nil {
		return nil, err
	}
	return d, nil
}

// Close closes the pool.
func (d *DB) Close() { d.pool.Close() }

// Pool is the pool, for the stream ID generator.
func (d *DB) Pool() *pgxpool.Pool { return d.pool }

const maxAttempts = 5

// runTxn is runInteraction: REPEATABLE READ, run again on a serialization
// failure or deadlock, at most maxAttempts times in all.
func (d *DB) runTxn(ctx context.Context, fn func(pgx.Tx) error) error {
	var err error
	for range maxAttempts {
		err = pgx.BeginTxFunc(ctx, d.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, fn)
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || (pg.Code != "40001" && pg.Code != "40P01") {
			return err
		}
	}
	return err
}

// Limits is push_rules.limits.
type Limits struct {
	RuleCount, RuleIDLength, RuleSize int
}

// DefaultLimits are PushRulesLimitsConfig's.
var DefaultLimits = Limits{RuleCount: 10_000, RuleIDLength: 300, RuleSize: 1024}

// RuleError is a SynapseError the store raises: its status, errcode and
// message as the client gets them.
type RuleError struct {
	Status  int
	Errcode string
	Msg     string
}

func (e *RuleError) Error() string { return e.Msg }

// ErrRuleNotFound is RuleNotFoundException where the servlet renders it as
// NotFoundError("Unknown rule"); a relative rule that is missing is a
// *RuleError (400) instead, as add_push_rule's caller renders it.
var ErrRuleNotFound = errors.New("Push rule does not exist.")

// ErrNoRow is simple_delete_one_txn's StoreError(404): DELETE of a rule
// that is not there.
var ErrNoRow = errors.New("No row found")

// Write is one write's stream bookkeeping.
type Write struct {
	Instance            string
	StreamID            int64
	EventStreamOrdering int64
}

// streamRow is _insert_push_rules_update_txn.
func streamRow(ctx context.Context, tx pgx.Tx, w Write, userID, ruleID, op string, class, priority *int, conditions, actions *string) error {
	_, err := tx.Exec(ctx, `INSERT INTO push_rules_stream
		(instance_name, stream_id, event_stream_ordering, user_id, rule_id, op, priority_class, priority, conditions, actions)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		w.Instance, w.StreamID, w.EventStreamOrdering, userID, ruleID, op, class, priority, conditions, actions)
	return err
}

// upsertRule is _upsert_push_rule_txn.
func (d *DB) upsertRule(ctx context.Context, tx pgx.Tx, lim Limits, w Write, userID, ruleID string, class, priority int,
	conditions, actions string, updateStream bool) error {
	tag, err := tx.Exec(ctx, `UPDATE push_rules SET priority_class = $1, priority = $2, conditions = $3, actions = $4
		WHERE user_name = $5 AND rule_id = $6`, class, priority, conditions, actions, userID, ruleID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var n int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM push_rules WHERE user_name = $1`, userID).Scan(&n); err != nil {
			return err
		}
		if n >= lim.RuleCount {
			return &RuleError{400, "M_UNKNOWN", fmt.Sprintf(
				"Creating a push rule would exceed the limit on the number of push rules associated with your account (%d rules > %d rules)",
				n+1, lim.RuleCount)}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO push_rules (id, user_name, rule_id, priority_class, priority, conditions, actions)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, d.ruleIDs.get(), userID, ruleID, class, priority, conditions, actions); err != nil {
			return err
		}
	}
	if updateStream {
		if err := streamRow(ctx, tx, w, userID, ruleID, "ADD", &class, &priority, &conditions, &actions); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO push_rules_enable (id, user_name, rule_id, enabled) VALUES ($1, $2, $3, 1)
		ON CONFLICT DO NOTHING`, d.enableIDs.get(), userID, ruleID)
	return err
}

// AddRule is add_push_rule's transaction (the caller has checked the
// limits on the ID and body): relative to before or after (before wins)
// when one is given, else at the top of its class.
func (d *DB) AddRule(ctx context.Context, lim Limits, w Write, userID, ruleID string, class int, conditions, actions, before, after string) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		if before == "" && after == "" {
			// FOR UPDATE on the rows, then the aggregate.
			if _, err := tx.Exec(ctx, `SELECT * FROM push_rules WHERE user_name = $1 AND priority_class = $2 FOR UPDATE`, userID, class); err != nil {
				return err
			}
			var n int
			var highest *int
			if err := tx.QueryRow(ctx, `SELECT COUNT(*), MAX(priority) FROM push_rules WHERE user_name = $1 AND priority_class = $2`,
				userID, class).Scan(&n, &highest); err != nil {
				return err
			}
			prio := 0
			if n > 0 && highest != nil {
				prio = *highest + 1
			}
			return d.upsertRule(ctx, tx, lim, w, userID, ruleID, class, prio, conditions, actions, true)
		}
		relative := before
		if relative == "" {
			relative = after
		}
		var basePrio, baseClass int
		err := tx.QueryRow(ctx, `SELECT priority, priority_class FROM push_rules WHERE user_name = $1 AND rule_id = $2 FOR UPDATE`,
			userID, relative).Scan(&basePrio, &baseClass)
		if errors.Is(err, pgx.ErrNoRows) {
			return &RuleError{400, "M_UNKNOWN", "before/after rule not found: " + relative}
		}
		if err != nil {
			return err
		}
		if baseClass != class {
			return &RuleError{400, "M_UNKNOWN", "Given priority class does not match class of relative rule"}
		}
		prio := basePrio
		if before != "" {
			prio = basePrio + 1
		}
		if _, err := tx.Exec(ctx, `UPDATE push_rules SET priority = priority + 1 WHERE user_name = $1 AND priority_class = $2 AND priority >= $3`,
			userID, class, prio); err != nil {
			return err
		}
		return d.upsertRule(ctx, tx, lim, w, userID, ruleID, class, prio, conditions, actions, true)
	})
}

// DeleteRule is delete_push_rule's transaction; ErrNoRow when the rule is
// not there (the enable row's delete is rolled back with it).
func (d *DB) DeleteRule(ctx context.Context, w Write, userID, ruleID string) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM push_rules_enable WHERE user_name = $1 AND rule_id = $2`, userID, ruleID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM push_rules WHERE user_name = $1 AND rule_id = $2`, userID, ruleID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNoRow
		}
		if tag.RowsAffected() > 1 {
			return errors.New("More than one row matched")
		}
		return streamRow(ctx, tx, w, userID, ruleID, "DELETE", nil, nil, nil, nil)
	})
}

// SetEnabled is _set_push_rule_enabled_txn.
func (d *DB) SetEnabled(ctx context.Context, w Write, userID, ruleID string, enabled, isDefault bool) error {
	newID := d.enableIDs.get()
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		if !isDefault {
			var one int
			err := tx.QueryRow(ctx, `SELECT 1 FROM push_rules WHERE user_name = $1 AND rule_id = $2 FOR KEY SHARE`, userID, ruleID).Scan(&one)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrRuleNotFound
			}
			if err != nil {
				return err
			}
		}
		v := 0
		if enabled {
			v = 1
		}
		if _, err := tx.Exec(ctx, `INSERT INTO push_rules_enable (user_name, rule_id, enabled, id) VALUES ($1, $2, $3, $4)
			ON CONFLICT (user_name, rule_id) DO UPDATE SET enabled = EXCLUDED.enabled`, userID, ruleID, v, newID); err != nil {
			return err
		}
		op := "DISABLE"
		if enabled {
			op = "ENABLE"
		}
		return streamRow(ctx, tx, w, userID, ruleID, op, nil, nil, nil, nil)
	})
}

// SetActions is set_push_rule_actions's transaction. A server-default
// rule's actions are stored as a row of class -1 (no stream row of its
// own: update_stream=False), anything else replaces the stored rule's.
func (d *DB) SetActions(ctx context.Context, lim Limits, w Write, userID, ruleID, actions string, isDefault bool) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		if isDefault {
			if err := d.upsertRule(ctx, tx, lim, w, userID, ruleID, -1, 1, "[]", actions, false); err != nil {
				return err
			}
		} else {
			var conditions string
			err := tx.QueryRow(ctx, `SELECT conditions FROM push_rules WHERE user_name = $1 AND rule_id = $2`, userID, ruleID).Scan(&conditions)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrRuleNotFound
			}
			if err != nil {
				return err
			}
			if size := len(conditions) + len(actions); size > lim.RuleSize {
				return &RuleError{413, "M_UNKNOWN", fmt.Sprintf("Push rule size exceeds server limit (%d bytes > %d bytes).", size, lim.RuleSize)}
			}
			if _, err := tx.Exec(ctx, `UPDATE push_rules SET actions = $1 WHERE user_name = $2 AND rule_id = $3`, actions, userID, ruleID); err != nil {
				return err
			}
		}
		return streamRow(ctx, tx, w, userID, ruleID, "ACTIONS", nil, nil, nil, &actions)
	})
}

// UserRules is get_push_rules_for_user's rows: sorted (priority_class
// DESC, priority DESC), and the enabled flags.
func (d *DB) UserRules(ctx context.Context, userID string) ([]pushrules.UserRule, map[string]bool, error) {
	rows, err := d.pool.Query(ctx, `SELECT rule_id, priority_class, priority, conditions, actions
		FROM push_rules WHERE user_name = $1 ORDER BY priority_class DESC, priority DESC`, userID)
	if err != nil {
		return nil, nil, err
	}
	rules, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (pushrules.UserRule, error) {
		var u pushrules.UserRule
		return u, r.Scan(&u.RuleID, &u.PriorityClass, &u.Priority, &u.Conditions, &u.Actions)
	})
	if err != nil {
		return nil, nil, err
	}
	erows, err := d.pool.Query(ctx, `SELECT rule_id, enabled FROM push_rules_enable WHERE user_name = $1`, userID)
	if err != nil {
		return nil, nil, err
	}
	enabled := map[string]bool{}
	for erows.Next() {
		var id string
		var e *int
		if err := erows.Scan(&id, &e); err != nil {
			erows.Close()
			return nil, nil, err
		}
		enabled[id] = e != nil && *e != 0
	}
	erows.Close()
	return rules, enabled, erows.Err()
}

// Update is one row of the stream.
type Update struct {
	Token int64
	Row   json.RawMessage
}

// Updates is get_all_push_rule_updates: rows (from, upto] by stream_id,
// at most limit; exactly limit rows is limited, upto moved to the last.
func (d *DB) Updates(ctx context.Context, from, upto int64, limit int) ([]Update, int64, bool, error) {
	if from == upto {
		return nil, upto, false, nil
	}
	rows, err := d.pool.Query(ctx, `SELECT stream_id, user_id FROM push_rules_stream
		WHERE $1 < stream_id AND stream_id <= $2 ORDER BY stream_id ASC LIMIT $3`, from, upto, limit)
	if err != nil {
		return nil, 0, false, err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Update, error) {
		var u Update
		var user string
		if err := r.Scan(&u.Token, &user); err != nil {
			return u, err
		}
		u.Row, _ = json.Marshal([]string{user})
		return u, nil
	})
	if err != nil {
		return nil, 0, false, err
	}
	if len(out) == limit {
		return out, out[len(out)-1].Token, true, nil
	}
	return out, upto, false, nil
}
