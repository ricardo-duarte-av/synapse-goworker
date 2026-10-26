// Command goworker is one Go process for the Synapse work that
// gosync-worker, goinbound-worker, gofederation-worker and gopro-worker did
// separately: client /sync, inbound federation /send, the federation read
// endpoints, and the outbound federation sender.
//
// It has no switches for which of those it does. It serves whatever nginx
// routes to it, and it sends federation only while homeserver.yaml names it in
// federation_sender_instances.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

// Stamped by the build. The same three names as the workers this replaces.
var (
	tag       = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	var (
		configPath  = flag.String("config", "goworker.yaml", "path to the configuration file")
		check       = flag.Bool("check", false, "validate the configuration and connections, then exit")
		showVersion = flag.Bool("version", false, "print build information and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("goworker %s (%s, built %s)\n", tag, commit, buildTime)
		return
	}

	if err := run(*configPath, *check); err != nil {
		fmt.Fprintf(os.Stderr, "goworker: %v\n", err)
		os.Exit(1)
	}
}

func run(configPath string, check bool) error {
	if _, err := os.Stat(configPath); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	// The platform (phase 1) and the components (phase 2) are not ported yet.
	return errors.New("not implemented yet: nothing is ported")
}
