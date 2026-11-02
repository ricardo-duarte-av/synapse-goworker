package fedping

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/federation"
	"maunium.net/go/mautrix/id"
)

// pending is a ping we started. Until the answer arrives a pong cannot be
// matched to it by question, so a pong from its destination is held as
// early: the MSC does not forbid answering the pong before the ping's
// response is read.
type pending struct {
	destination string
	answer      string
	early       []pongSeen
	pong        *pongSeen
	done        chan struct{}
}

type pongSeen struct {
	question string
	details  []string
	at       time.Time
}

// Attempt is one ping request.
type Attempt struct {
	Path          string   `json:"path"`
	Authenticated bool     `json:"authenticated"`
	Status        int      `json:"status,omitempty"`
	ErrCode       string   `json:"errcode,omitempty"`
	Error         string   `json:"error,omitempty"`
	Answer        string   `json:"answer,omitempty"`
	Details       []string `json:"details,omitempty"`
	TookMS        int64    `json:"took_ms"`
}

// Report is what a ping found.
type Report struct {
	Destination string    `json:"destination"`
	Question    string    `json:"question"`
	Attempts    []Attempt `json:"attempts"`
	// Answered is true when the destination answered the question.
	Answered bool `json:"answered"`
	// Ponged is true when the destination then confirmed the answer to us,
	// signed: federation works both ways.
	Ponged      bool     `json:"ponged"`
	PongAfterMS int64    `json:"pong_after_ms,omitempty"`
	PongDetails []string `json:"pong_details,omitempty"`
	Conclusion  string   `json:"conclusion"`
}

// Initiate pings destination and waits up to wait for its pong.
//
// The ping is signed. If the signature is refused it is retried unsigned,
// naming us as origin, so the destination can say why. Each request goes to
// the unstable path, then the stable one if that is not recognised.
func (s *Server) Initiate(ctx context.Context, destination string, wait time.Duration) Report {
	question := randomToken()
	rep := Report{Destination: destination, Question: question}
	p := &pending{destination: destination, done: make(chan struct{})}
	s.mu.Lock()
	s.pending[question] = p
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.pending, question)
		s.mu.Unlock()
	}()

	var answer string
	for _, authenticated := range []bool{true, false} {
		a := s.ping(ctx, destination, question, authenticated)
		rep.Attempts = append(rep.Attempts, a...)
		last := a[len(a)-1]
		if last.Answer != "" {
			answer = last.Answer
			break
		}
		if last.Status != http.StatusUnauthorized {
			break // only a refused signature is worth retrying unsigned
		}
	}
	if answer == "" {
		rep.Conclusion = conclusion(rep)
		return rep
	}
	rep.Answered = true
	start := time.Now()

	s.mu.Lock()
	p.answer = answer
	for i := range p.early {
		if p.early[i].question == answer {
			p.pong = &p.early[i]
			close(p.done)
			break
		}
	}
	p.early = nil
	s.mu.Unlock()

	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-p.done:
	case <-t.C:
	case <-ctx.Done():
	}
	s.mu.Lock()
	if pong := p.pong; pong != nil {
		rep.Ponged = true
		rep.PongDetails = pong.details
		rep.PongAfterMS = max(0, pong.at.Sub(start).Milliseconds())
	}
	s.mu.Unlock()
	rep.Conclusion = conclusion(rep)
	return rep
}

// ping asks once, on the unstable path and then, if it is not recognised,
// the stable one.
func (s *Server) ping(ctx context.Context, destination, question string, authenticated bool) []Attempt {
	req := pingRequest{Question: question}
	if !authenticated {
		req.Origin = s.serverName
	}
	var out []Attempt
	for _, path := range []mautrix.BaseURLPath{unstablePath("ping"), stablePath("ping")} {
		var resp pingResponse
		start := time.Now()
		rctx, cancel := context.WithTimeout(ctx, pongTimeout)
		_, hresp, err := s.client.MakeFullRequest(rctx, federation.RequestParams{
			ServerName: destination, Method: http.MethodPost, Path: path,
			Authenticate: authenticated, RequestJSON: req, ResponseJSON: &resp,
		})
		cancel()
		a := Attempt{
			Path:          "/" + joinPath(path),
			Authenticated: authenticated,
			TookMS:        time.Since(start).Milliseconds(),
		}
		if hresp != nil {
			a.Status = hresp.StatusCode
		}
		sent.WithLabelValues("ping", outcomeOf(err)).Inc()
		if err != nil {
			var herr mautrix.HTTPError
			if errors.As(err, &herr) && herr.RespError != nil {
				a.ErrCode, a.Error = herr.RespError.ErrCode, herr.RespError.Err
			} else {
				a.Error = err.Error()
			}
		} else {
			a.Answer, a.Details = resp.Answer, resp.Details
			if a.Answer == "" {
				a.Error = "answered without an answer"
			}
		}
		out = append(out, a)
		unrecognised := (a.Status == http.StatusNotFound || a.Status == http.StatusMethodNotAllowed) &&
			(a.ErrCode == "" || a.ErrCode == "M_UNRECOGNIZED")
		if !unrecognised {
			break
		}
	}
	return out
}

// matchPong records a pong from origin for one of our pings, reporting
// whether there was one.
func (s *Server) matchPong(origin, question string, details []string) bool {
	seen := pongSeen{question: question, details: details, at: s.now()}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.pending {
		if p.destination != origin || p.pong != nil {
			continue
		}
		if p.answer == question {
			p.pong = &seen
			close(p.done)
			return true
		}
		if p.answer == "" && len(p.early) < 4 {
			p.early = append(p.early, seen)
			return true
		}
	}
	return false
}

func conclusion(r Report) string {
	last := r.Attempts[len(r.Attempts)-1]
	switch {
	case r.Ponged:
		if last.Authenticated {
			return "Federation works both ways: our signed ping was answered and the answer confirmed by a signed pong."
		}
		return "Both servers can reach each other, but " + r.Destination + " refused our signature; see the details of its answer."
	case r.Answered:
		return r.Destination + " answered but no pong reached us: it may not reach us, or our signature checks refuse it (see the logs)."
	case last.Status == http.StatusNotFound || last.Status == http.StatusMethodNotAllowed:
		return r.Destination + " does not serve MSC4524 (or has it disabled)."
	case last.Status == http.StatusTooManyRequests:
		return r.Destination + " rate-limited the ping; try again in a minute."
	case last.Status == 0:
		return "We could not reach " + r.Destination + ": " + last.Error
	}
	return r.Destination + " did not answer the ping."
}

func joinPath(p mautrix.BaseURLPath) string {
	parts := make([]string, len(p))
	for i, v := range p {
		parts[i] = v.(string)
	}
	return strings.Join(parts, "/")
}

// DebugHandler serves POST ?server=<name>[&wait=<duration>] on the metrics
// listener: it pings server and answers with the Report.
func (s *Server) DebugHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST ?server=<server name>[&wait=30s]", http.StatusMethodNotAllowed)
			return
		}
		server := r.URL.Query().Get("server")
		if !id.ValidateServerName(server) {
			http.Error(w, "server must be a server name", http.StatusBadRequest)
			return
		}
		wait := 30 * time.Second
		if v := r.URL.Query().Get("wait"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 || d > 5*time.Minute {
				http.Error(w, "wait must be a duration up to 5m", http.StatusBadRequest)
				return
			}
			wait = d
		}
		rep := s.Initiate(r.Context(), server, wait)
		s.log.Info().Str("destination", server).Bool("answered", rep.Answered).Bool("ponged", rep.Ponged).
			Msg("MSC4524 ping finished")
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
	})
}
