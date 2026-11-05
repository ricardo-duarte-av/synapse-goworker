package devicelists

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tidwall/gjson"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/clientauth"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/pyjson"
)

// UIAStore is what user-interactive authentication reads and writes (DB):
// storage/databases/main/ui_auth.py and the access-token and user rows
// handlers/auth.py consults.
type UIAStore interface {
	TokenLastValidated(ctx context.Context, tokenID int64) (int64, error)
	SetTokenLastValidated(ctx context.Context, tokenID, nowMS int64) (bool, error)
	PasswordHashes(ctx context.Context, userID string) ([][2]*string, error)
	UserByThreepid(ctx context.Context, medium, address string) (string, error)
	CreateUIASession(ctx context.Context, id, clientdict, uri, method, description string, nowMS int64) (bool, error)
	SetUIASessionData(ctx context.Context, id, key, valueJSON string) error
	UIASession(ctx context.Context, id string) (*UIASession, error)
	SetUIAClientdict(ctx context.Context, id, clientdict string) error
	AddUIAUserAgentIP(ctx context.Context, id, userAgent, ip string) error
	MarkUIAStageComplete(ctx context.Context, id, stage, resultJSON string) error
	CompletedUIAStages(ctx context.Context, id string) ([][2]string, error)
}

// UIASession is a ui_auth_sessions row.
type UIASession struct {
	ClientDict  string
	URI, Method string
}

// Synapse's constants (api/constants.py, handlers/auth.py).
const (
	loginPassword             = "m.login.password"
	invalidUsernameOrPassword = "Invalid username or password"
	uiaRequestUserID          = "request_user_id"
	sessionIDLength           = 24
)

// uiaChallenge is InteractiveAuthIncompleteError: the 401 and its body.
type uiaChallenge struct{ body string }

func (c *uiaChallenge) Error() string {
	return "devicelists: user-interactive authentication incomplete"
}

// uiaToMain says whether a request's UIA is one only main can run: a
// password provider or SSO configured, a stage needing a checker of
// Synapse's we do not reproduce (recaptcha, email, msisdn, registration
// token: none is offered here, so only a client that sends one anyway
// lands on main), a phone identifier (phonenumbers' parsing), or a
// requester without an access token row (a /whoami verdict).
func (a *App) uiaToMain(v clientauth.Verdict, auth gjson.Result) bool {
	u := a.Synapse().UIA
	if u.SSO || u.PasswordProviders || a.Synapse().ClientAuth.Modules > 0 {
		return true
	}
	if v.AppServiceID == "" && v.TokenID == 0 {
		return true
	}
	switch pyField(auth, "type").Str {
	case "m.login.recaptcha", "m.login.email.identity", "m.login.msisdn", "m.login.registration_token":
		return true
	}
	id := pyField(auth, "identifier")
	return id.IsObject() && pyField(id, "type").Str == "m.id.phone"
}

// validateUserViaUIA is validate_user_via_ui_auth: nil once the requester has re-authenticated (or did within
// ui_auth.session_timeout); a *uiaChallenge for the 401; a *sigError for
// Synapse's other refusals.
//
// The failed-attempts ratelimiter it consults is never updated (the
// LoginError it counts is caught inside check_ui_auth), so it never
// refuses and is not reproduced.
func (a *App) validateUserViaUIA(ctx context.Context, r *http.Request, v clientauth.Verdict, body gjson.Result, description string, canSkip bool) error {
	if v.TokenID == 0 {
		return errors.New("devicelists: cannot validate a user without an access token")
	}
	u := a.Synapse().UIA
	now := a.Now().UnixMilli()
	if canSkip && u.SessionTimeoutMS != 0 {
		last, err := a.UStore.TokenLastValidated(ctx, v.TokenID)
		if err != nil {
			return err
		}
		if now-last < u.SessionTimeoutMS {
			return nil
		}
	}
	var types []string
	if u.LocalDB && u.PasswordForReauth {
		_, hash, found, err := a.findUserAndHash(ctx, v.UserID)
		if err != nil {
			return err
		}
		if found && hash != "" {
			types = append(types, loginPassword)
		}
	}
	creds, err := a.checkUIA(ctx, r, types, body, description, v.UserID)
	if err != nil {
		return err
	}
	var validated *gjson.Result
	for _, t := range types {
		if c, ok := creds.get(t); ok {
			validated = &c
			break
		}
	}
	if validated == nil {
		return errors.New("check_auth returned True but no successful login type")
	}
	if validated.Type != gjson.String || validated.Str != v.UserID {
		return &sigError{403, "M_FORBIDDEN", "Invalid auth"}
	}
	ok, err := a.UStore.SetTokenLastValidated(ctx, v.TokenID, a.Now().UnixMilli())
	if err != nil {
		return err
	}
	if !ok {
		return &sigError{404, "M_UNKNOWN", "No row found"}
	}
	return nil
}

// checkUIA is check_ui_auth for single-stage flows ([[t] for t in types]):
// the completed stages once one flow is complete.
func (a *App) checkUIA(ctx context.Context, r *http.Request, types []string, body gjson.Result, description, userID string) (*omap[gjson.Result], error) {
	authdict := pyField(body, "auth")
	if authdict.Type == gjson.Null || !authdict.Exists() {
		authdict = gjson.Parse(`{}`)
	}
	clientdict, err := withoutKey(body, "auth")
	if err != nil {
		return nil, err
	}
	sid := ""
	if s := pyField(authdict, "session"); s.Exists() {
		sid = s.Str // a string or null: validated
	}
	uri, method := r.RequestURI, r.Method
	if sid == "" {
		if sid, err = a.createUIASession(ctx, clientdict, uri, method, description); err != nil {
			return nil, err
		}
		if err := a.UStore.SetUIASessionData(ctx, sid, uiaRequestUserID, pyDumpsStr(userID)); err != nil {
			return nil, err
		}
	} else {
		sess, err := a.UStore.UIASession(ctx, sid)
		if err != nil {
			return nil, err
		}
		if sess == nil {
			return nil, &sigError{400, "M_UNKNOWN", "Unknown session ID: " + sid}
		}
		if !clientTruthy(gjson.Parse(clientdict)) {
			clientdict = sess.ClientDict
		}
		if sess.URI != uri || sess.Method != method {
			return nil, &sigError{403, "M_UNKNOWN", "Requested operation has changed during the UI authentication session."}
		}
		enc, err := pyjson.Encode([]byte(clientdict))
		if err != nil {
			return nil, err
		}
		if err := a.UStore.SetUIAClientdict(ctx, sid, enc); err != nil {
			return nil, err
		}
	}
	if err := a.UStore.AddUIAUserAgentIP(ctx, sid, userAgent(r), clientauth.ClientIP(r)); err != nil {
		return nil, err
	}
	if !clientTruthy(authdict) {
		return nil, &uiaChallenge{authForFlows(types, sid, false, nil, "")}
	}
	errordict := ""
	if lt := pyField(authdict, "type"); lt.Exists() {
		result, err := a.checkAuthDict(ctx, authdict)
		var le *loginError
		switch {
		case errors.As(err, &le):
			// error_dict is cs_error: error, then errcode.
			errordict = `"error":` + jsonStr(le.msg) + `,"errcode":` + jsonStr(le.errcode)
		case err != nil:
			return nil, err
		case clientTruthy(gjson.Parse(result)):
			// mark_ui_auth_stage_complete: the type is a string here (a
			// checker's, or validate_login checked it).
			if err := a.UStore.MarkUIAStageComplete(ctx, sid, lt.Str, result); err != nil {
				return nil, err
			}
		}
	}
	rows, err := a.UStore.CompletedUIAStages(ctx, sid)
	if err != nil {
		return nil, err
	}
	creds := &omap[gjson.Result]{}
	for _, row := range rows {
		creds.set(row[0], gjson.Parse(row[1]))
	}
	for _, t := range types {
		if _, ok := creds.get(t); ok {
			return creds, nil
		}
	}
	return nil, &uiaChallenge{authForFlows(types, sid, true, creds.keys, errordict)}
}

// authForFlows is _auth_dict_for_flows; past the first challenge also
// "completed" and the failed stage's error.
func authForFlows(types []string, sid string, withCompleted bool, completed []string, errordict string) string {
	var b strings.Builder
	b.WriteString(`{"session":` + jsonStr(sid) + `,"flows":[`)
	for i, t := range types {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"stages":[` + jsonStr(t) + `]}`)
	}
	b.WriteString(`],"params":{}`)
	if withCompleted {
		b.WriteString(`,"completed":[`)
		for i, c := range completed {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(jsonStr(c))
		}
		b.WriteByte(']')
	}
	if errordict != "" {
		b.WriteString("," + errordict)
	}
	b.WriteByte('}')
	return b.String()
}

// createUIASession is create_ui_auth_session: five tries at a fresh ID.
func (a *App) createUIASession(ctx context.Context, clientdict, uri, method, description string) (string, error) {
	enc, err := pyjson.Encode([]byte(clientdict))
	if err != nil {
		return "", err
	}
	for range 5 {
		id := randomString(sessionIDLength)
		ok, err := a.UStore.CreateUIASession(ctx, id, enc, uri, method, description, a.Now().UnixMilli())
		if err != nil {
			return "", err
		}
		if ok {
			return id, nil
		}
	}
	return "", &sigError{500, "M_UNKNOWN", "Couldn't generate a session ID."}
}

// randomString is stringutils.random_string: ASCII letters.
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		x, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		b[i] = letters[x.Int64()]
	}
	return string(b)
}

// userAgent is get_request_user_agent: the last User-Agent, or "".
func userAgent(r *http.Request) string {
	vs := r.Header.Values("User-Agent")
	if len(vs) == 0 {
		return ""
	}
	return vs[len(vs)-1]
}

// withoutKey is the dict without one key, its order kept.
func withoutKey(obj gjson.Result, key string) (string, error) {
	var b strings.Builder
	b.WriteByte('{')
	n := 0
	obj.ForEach(func(k, v gjson.Result) bool {
		if k.Str == key {
			return true
		}
		if n > 0 {
			b.WriteByte(',')
		}
		n++
		b.WriteString(jsonStr(k.Str) + ":" + v.Raw)
		return true
	})
	b.WriteByte('}')
	return b.String(), nil
}

// loginError is a LoginError: caught by check_ui_auth and merged into the
// 401 for another try.
type loginError struct {
	status       int
	errcode, msg string
}

func (e *loginError) Error() string { return e.msg }

var errBadLogin = &loginError{403, "M_FORBIDDEN", invalidUsernameOrPassword}

// checkAuthDict is _check_auth_dict: m.login.dummy and m.login.terms
// always pass; anything else is validate_login(is_reauth=True). The
// result is JSON.
func (a *App) checkAuthDict(ctx context.Context, authdict gjson.Result) (string, error) {
	lt := pyField(authdict, "type")
	if lt.IsObject() || lt.IsArray() {
		return "", errPython // checkers.get(unhashable)
	}
	if lt.Type == gjson.String && (lt.Str == "m.login.dummy" || lt.Str == "m.login.terms") {
		return "true", nil
	}
	id, err := a.validateLogin(ctx, authdict)
	if err != nil {
		return "", err
	}
	return pyDumpsStr(id), nil
}

// validateLogin is validate_login(login_submission, is_reauth=True)
// without password providers (with any, UIA goes to main).
func (a *App) validateLogin(ctx context.Context, sub gjson.Result) (string, error) {
	u := a.Synapse().UIA
	lt := pyField(sub, "type")
	if lt.Type != gjson.String {
		return "", &sigError{400, "M_INVALID_PARAM", "Bad parameter: type"}
	}
	password := pyField(sub, "password")
	if lt.Str == loginPassword {
		if !u.PasswordForReauth {
			return "", &sigError{400, "M_UNKNOWN", "Password login has been disabled."}
		}
		if password.Type != gjson.String {
			return "", &sigError{400, "M_INVALID_PARAM", "Bad parameter: password"}
		}
	}
	// convert_client_dict_legacy_fields_to_identifier
	identifier := pyField(sub, "identifier")
	if !identifier.Exists() {
		identifier = gjson.Parse(`{}`)
	}
	if user := pyField(sub, "user"); clientTruthy(user) {
		identifier = gjson.Parse(`{"type":"m.id.user","user":` + user.Raw + `}`)
	}
	medium, address := pyField(sub, "medium"), pyField(sub, "address")
	if clientTruthy(medium) && clientTruthy(address) {
		identifier = gjson.Parse(`{"type":"m.id.thirdparty","medium":` + medium.Raw + `,"address":` + address.Raw + `}`)
	}
	if !clientTruthy(identifier) {
		return "", &sigError{400, "M_INVALID_PARAM", "Invalid login submission"}
	}
	in, err := pyIn(identifier, "type")
	if err != nil {
		return "", err
	}
	if !in {
		return "", &sigError{400, "M_MISSING_PARAM", "'identifier' dict has no key 'type'"}
	}
	idType, _, err := pyIndex(identifier, "type")
	if err != nil {
		return "", err
	}
	if pyEqual(idType, gjson.Parse(`"m.id.thirdparty"`)) {
		address, medium := pyField(identifier, "address"), pyField(identifier, "medium")
		if !address.Exists() || address.Type == gjson.Null || !medium.Exists() || medium.Type == gjson.Null {
			return "", &sigError{400, "M_UNKNOWN", "Invalid thirdparty identifier"}
		}
		if medium.Type != gjson.String || address.Type != gjson.String {
			return "", errPython
		}
		addr := address.Str
		if medium.Str == "email" {
			if addr, err = canonicaliseEmail(addr); err != nil {
				return "", &sigError{400, "M_UNKNOWN", err.Error()}
			}
		}
		userID, err := a.UStore.UserByThreepid(ctx, medium.Str, addr)
		if err != nil {
			return "", err
		}
		if userID == "" {
			return "", errBadLogin
		}
		identifier = gjson.Parse(`{"type":"m.id.user","user":` + jsonStr(userID) + `}`)
		idType = pyField(identifier, "type")
	}
	if !pyEqual(idType, gjson.Parse(`"m.id.user"`)) {
		return "", &sigError{400, "M_UNKNOWN", "Unknown login identifier type"}
	}
	username := pyField(identifier, "user")
	if !clientTruthy(username) {
		return "", &sigError{400, "M_UNKNOWN", "User identifier is missing 'user' key"}
	}
	if username.Type != gjson.String {
		return "", errPython // .startswith
	}
	qualified := username.Str
	if !strings.HasPrefix(qualified, "@") {
		qualified = "@" + qualified + ":" + a.serverName
	}
	// _validate_userid_login
	if lt.Str != loginPassword || !u.LocalDB {
		return "", &sigError{400, "M_UNKNOWN", "Unknown login type " + lt.Str}
	}
	canonical, hash, found, err := a.findUserAndHash(ctx, qualified)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errBadLogin
	}
	ok, err := validateHash(password.Str, u.Pepper, hash)
	if err != nil {
		return "", err
	}
	if !ok {
		a.Log.Warn().Str("user", canonical).Msg("Failed password login for user")
		return "", errBadLogin
	}
	return canonical, nil
}

// findUserAndHash is _find_user_id_and_pwd_hash: the one user whose name
// matches case-insensitively, or the exact one of several.
func (a *App) findUserAndHash(ctx context.Context, userID string) (string, string, bool, error) {
	rows, err := a.UStore.PasswordHashes(ctx, userID)
	if err != nil {
		return "", "", false, err
	}
	// dict(rows): a repeated name keeps its first place and last hash.
	m := &omap[*string]{}
	for _, r := range rows {
		m.set(*r[0], r[1])
	}
	pick := func(name string) (string, string, bool, error) {
		h := m.vals[name]
		if h == nil {
			return name, "", true, nil
		}
		return name, *h, true, nil
	}
	switch {
	case len(m.keys) == 0:
		return "", "", false, nil
	case len(m.keys) == 1:
		return pick(m.keys[0])
	}
	if _, ok := m.get(userID); ok {
		return pick(userID)
	}
	return "", "", false, nil
}

// validateHash is validate_hash: NFKC, the pepper, bcrypt's 72 bytes. A
// hash bcrypt cannot read is Synapse's ValueError, a 500.
func validateHash(password, pepper, hash string) (bool, error) {
	if hash == "" {
		return false, nil
	}
	for i := 0; i < len(hash); i++ {
		if hash[i] >= 0x80 {
			return false, errPython // stored_hash.encode("ascii")
		}
	}
	b := []byte(norm.NFKC.String(password) + pepper)
	if len(b) > 72 {
		b = b[:72]
	}
	err := bcrypt.CompareHashAndPassword([]byte(hash), b)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, bcrypt.ErrMismatchedHashAndPassword):
		return false, nil
	}
	return false, err
}

// canonicaliseEmail is canonicalise_email: the local part case-folded,
// the domain lower-cased.
func canonicaliseEmail(address string) (string, error) {
	address = strings.TrimSpace(pyStrip(address))
	parts := strings.Split(address, "@")
	if len(parts) != 2 {
		return "", errors.New("Unable to parse email address")
	}
	return cases.Fold().String(parts[0]) + "@" + cases.Lower(language.Und).String(parts[1]), nil
}

// pyStrip is str.strip(): Python's whitespace (Unicode's, and the
// separators \x1c-\x1f).
func pyStrip(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		switch r {
		case ' ', '\t', '\n', '\v', '\f', '\r', 0x1c, 0x1d, 0x1e, 0x1f, 0x85, 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000:
			return true
		}
		return r >= 0x2000 && r <= 0x200a
	})
}

// Store.

// TokenLastValidated is get_access_token_last_validated (0 for NULL).
func (d *DB) TokenLastValidated(ctx context.Context, tokenID int64) (int64, error) {
	var last *int64
	err := d.pool.QueryRow(ctx, `SELECT last_validated FROM access_tokens WHERE id = $1`, tokenID).Scan(&last)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, &sigError{404, "M_UNKNOWN", "No row found"}
	}
	if err != nil || last == nil {
		return 0, err
	}
	return *last, nil
}

// SetTokenLastValidated is update_access_token_last_validated; false
// when the token is gone.
func (d *DB) SetTokenLastValidated(ctx context.Context, tokenID, nowMS int64) (bool, error) {
	tag, err := d.pool.Exec(ctx, `UPDATE access_tokens SET last_validated = $2 WHERE id = $1`, tokenID, nowMS)
	return tag.RowsAffected() > 0, err
}

// PasswordHashes is get_users_by_id_case_insensitive's rows.
func (d *DB) PasswordHashes(ctx context.Context, userID string) ([][2]*string, error) {
	rows, err := d.pool.Query(ctx, `SELECT name, password_hash FROM users WHERE lower(name) = lower($1)`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]*string
	for rows.Next() {
		var name string
		var hash *string
		if err := rows.Scan(&name, &hash); err != nil {
			return nil, err
		}
		out = append(out, [2]*string{&name, hash})
	}
	return out, rows.Err()
}

// UserByThreepid is get_user_id_by_threepid ("" when none).
func (d *DB) UserByThreepid(ctx context.Context, medium, address string) (string, error) {
	var u string
	err := d.pool.QueryRow(ctx, `SELECT user_id FROM user_threepids WHERE medium = $1 AND address = $2`, medium, address).Scan(&u)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return u, err
}

// CreateUIASession inserts a session; false when the ID is taken.
func (d *DB) CreateUIASession(ctx context.Context, id, clientdict, uri, method, description string, nowMS int64) (bool, error) {
	_, err := d.pool.Exec(ctx, `INSERT INTO ui_auth_sessions (session_id, clientdict, uri, method, description, serverdict, creation_time)
		VALUES ($1, $2, $3, $4, $5, '{}', $6)`, id, clientdict, uri, method, description, nowMS)
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		return false, nil
	}
	return err == nil, err
}

// SetUIASessionData is _set_ui_auth_session_data_txn.
func (d *DB) SetUIASessionData(ctx context.Context, id, key, valueJSON string) error {
	return d.runTxn(ctx, func(tx pgx.Tx) error {
		var server string
		err := tx.QueryRow(ctx, `SELECT serverdict FROM ui_auth_sessions WHERE session_id = $1`, id).Scan(&server)
		if errors.Is(err, pgx.ErrNoRows) {
			return &sigError{400, "M_UNKNOWN", "Unknown session ID: " + id}
		}
		if err != nil {
			return err
		}
		obj, err := withoutKey(gjson.Parse(server), key)
		if err != nil {
			return err
		}
		obj = strings.TrimSuffix(obj, "}")
		if obj != "{" {
			obj += ","
		}
		enc, err := pyjson.Encode([]byte(obj + jsonStr(key) + ":" + valueJSON + "}"))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE ui_auth_sessions SET serverdict = $2 WHERE session_id = $1`, id, enc)
		return err
	})
}

// UIASession is get_ui_auth_session (nil when there is none).
func (d *DB) UIASession(ctx context.Context, id string) (*UIASession, error) {
	var s UIASession
	err := d.pool.QueryRow(ctx, `SELECT clientdict, uri, method FROM ui_auth_sessions WHERE session_id = $1`, id).Scan(&s.ClientDict, &s.URI, &s.Method)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// SetUIAClientdict is set_ui_auth_clientdict.
func (d *DB) SetUIAClientdict(ctx context.Context, id, clientdict string) error {
	tag, err := d.pool.Exec(ctx, `UPDATE ui_auth_sessions SET clientdict = $2 WHERE session_id = $1`, id, clientdict)
	if err == nil && tag.RowsAffected() == 0 {
		return &sigError{404, "M_UNKNOWN", "No row found"}
	}
	return err
}

// AddUIAUserAgentIP is add_user_agent_ip_to_ui_auth_session.
func (d *DB) AddUIAUserAgentIP(ctx context.Context, id, userAgent, ip string) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO ui_auth_sessions_ips (session_id, user_agent, ip) VALUES ($1, $2, $3)
		ON CONFLICT (session_id, user_agent, ip) DO NOTHING`, id, userAgent, ip)
	return err
}

// MarkUIAStageComplete is mark_ui_auth_stage_complete.
func (d *DB) MarkUIAStageComplete(ctx context.Context, id, stage, resultJSON string) error {
	_, err := d.pool.Exec(ctx, `INSERT INTO ui_auth_sessions_credentials (session_id, stage_type, result) VALUES ($1, $2, $3)
		ON CONFLICT (session_id, stage_type) DO UPDATE SET result = EXCLUDED.result`, id, stage, resultJSON)
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23503" {
		return &sigError{400, "M_UNKNOWN", "Unknown session ID: " + id}
	}
	return err
}

// CompletedUIAStages is get_completed_ui_auth_stages: (stage, result).
func (d *DB) CompletedUIAStages(ctx context.Context, id string) ([][2]string, error) {
	rows, err := d.pool.Query(ctx, `SELECT stage_type, result FROM ui_auth_sessions_credentials WHERE session_id = $1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]string
	for rows.Next() {
		var s, r string
		if err := rows.Scan(&s, &r); err != nil {
			return nil, err
		}
		out = append(out, [2]string{s, r})
	}
	return out, rows.Err()
}
