package main

import (
	"fmt"
	"log"
	"os"
)

const usage = `Watchdog - CS2 behavioural anti-cheat

Usage:
  watchdog <command> [flags]

Commands:
  ingest <demo.dem>   Parse a demo and ingest all tick/event data into ClickHouse

Flags:
  --config <path>     Path to config file (default: config.yaml)

Examples:
  go run . ingest demos/match.dem
  go run . ingest --config /etc/watchdog.yaml demos/match.dem
`

func main() {
	log.SetFlags(log.Ltime | log.Lmsgprefix)
	log.SetPrefix("watchdog ")

	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Print(usage)
		os.Exit(1)
	}

	configPath := "config.yaml"

	// Strip --config flag from args before dispatching.
	args = parseFlags(args, &configPath)

	if len(args) == 0 {
		fmt.Print(usage)
		os.Exit(1)
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("error: load config %q: %v", configPath, err)
	}

	switch args[0] {
	case "ingest":
		if len(args) < 2 {
			log.Fatal("error: ingest requires a path to a .dem file")
		}
		demoPath := args[1]
		if err := IngestDemo(cfg, demoPath); err != nil {
			log.Fatalf("error: ingest %q: %v", demoPath, err)
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", args[0], usage)
		os.Exit(1)
	}
}

// parseFlags extracts --config <path> from args and returns the remainder.
func parseFlags(args []string, configPath *string) []string {
	out := args[:0]
	for i := 0; i < len(args); i++ {
		if args[i] == "--config" && i+1 < len(args) {
			*configPath = args[i+1]
			i++
		} else {
			out = append(out, args[i])
		}
	}
	return out
}
