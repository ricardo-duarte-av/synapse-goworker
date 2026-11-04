package clientauth

import (
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/appservice"
)

// appServices is what was loaded (platform/appservice, the one parser of
// the registrations): the services this worker can authenticate natively,
// and the tokens of those it cannot. A request with one of the latter goes
// to /whoami, so Synapse's own regex engine decides.
type appServices struct {
	byToken     map[string]*appservice.Service
	unsupported map[string]string // as_token -> why
}

// loadAppServices is load_appservices, split by whether a service's
// requests can be judged natively.
func loadAppServices(serverName string, files []string) (*appServices, error) {
	services, err := appservice.Load(files, serverName)
	if err != nil {
		return nil, err
	}
	out := &appServices{byToken: map[string]*appservice.Service{}, unsupported: map[string]string{}}
	for token, s := range services {
		if s.Unsupported != "" {
			out.unsupported[token] = s.Unsupported
			continue
		}
		out.byToken[token] = s
	}
	return out, nil
}
