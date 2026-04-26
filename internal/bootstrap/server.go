package bootstrap

import (
	"os"
	"time"
)

// ServerConfig holds all runtime configuration derived from environment variables.
type ServerConfig struct {
	// ListenAddr is the address the emulator listens on (LISTEN_ADDR, default ":8085").
	ListenAddr string
	// InitConfigPath is an optional path to a YAML resource init file (INIT_CONFIG).
	InitConfigPath string
	// OTELEndpoint is the OTLP HTTP collector endpoint for tracing
	// (OTEL_EXPORTER_OTLP_ENDPOINT). Empty means tracing is disabled.
	OTELEndpoint string
	// LogLevel controls the minimum log level (LOG_LEVEL: debug|info|warn|error, default "info").
	LogLevel string
	// LogFormat selects the log output format (LOG_FORMAT: text|json, default "text").
	LogFormat string
	// DeliveryDelay is an artificial delay applied before delivering newly published
	// messages to subscribers (PUBSUB_DELIVERY_DELAY, e.g. "50ms"). Zero by default.
	// Useful when testing against clients that assume real PubSub network latency.
	DeliveryDelay time.Duration
}

// LoadServerConfig reads ServerConfig from environment variables.
func LoadServerConfig() *ServerConfig {
	return &ServerConfig{
		ListenAddr:     getEnv("LISTEN_ADDR", ":8085"),
		InitConfigPath: os.Getenv("INIT_CONFIG"),
		OTELEndpoint:   os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		LogLevel:       getEnv("LOG_LEVEL", "info"),
		LogFormat:      getEnv("LOG_FORMAT", "json"),
		DeliveryDelay:  parseDuration(os.Getenv("PUBSUB_DELIVERY_DELAY")),
	}
}

func parseDuration(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
