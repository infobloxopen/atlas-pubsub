package main

import "github.com/spf13/pflag"

const (
	// configuration defaults support local development (i.e. "go run ...")

	// Server
	defaultServerAddress = "0.0.0.0"
	defaultServerPort    = "5555"

	// Health
	defaultInternalEnable    = true
	defaultInternalAddress   = "0.0.0.0"
	defaultInternalPort      = "8081"
	defaultInternalHealth    = "/health"
	defaultInternalReadiness = "/ready"

	// Logging
	defaultLoggingLevel = "debug"
)

var (
	// define flag overrides
	flagServerAddress = pflag.String("server.address", defaultServerAddress, "adress of gRPC server")
	flagServerPort    = pflag.String("server.port", defaultServerPort, "port of gRPC server")

	flagInternalEnable    = pflag.Bool("internal.enable", defaultInternalEnable, "enable internal http server")
	flagInternalAddress   = pflag.String("internal.address", defaultInternalAddress, "address of internal http server")
	flagInternalPort      = pflag.String("internal.port", defaultInternalPort, "port of internal http server")
	flagInternalHealth    = pflag.String("internal.health", defaultInternalHealth, "endpoint for health checks")
	flagInternalReadiness = pflag.String("internal.readiness", defaultInternalReadiness, "endpoint for readiness checks")

	flagLoggingLevel = pflag.String("logging.level", defaultLoggingLevel, "log level of application")
)
