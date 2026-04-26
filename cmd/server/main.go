package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpcreflect"
	"connectrpc.com/otelconnect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/fbufler/google-pubsub/gen/google/pubsub/v1/pubsubpbconnect"
	"github.com/fbufler/google-pubsub/internal/api/handler"
	"github.com/fbufler/google-pubsub/internal/core/storage/memory"
	"github.com/fbufler/google-pubsub/internal/core/storage/repositories"
	"github.com/fbufler/google-pubsub/internal/core/usecases"
	"github.com/fbufler/google-pubsub/internal/bootstrap"
	"github.com/fbufler/google-pubsub/internal/telemetry"
)

func main() {
	cfg := bootstrap.LoadServerConfig()

	// --- Logging ---
	if err := telemetry.SetupLogging(cfg.LogLevel, cfg.LogFormat); err != nil {
		slog.Error("failed to setup logging", "err", err)
		os.Exit(1)
	}

	// --- Tracing ---
	ctx := context.Background()
	shutdownTracing, err := telemetry.Setup(ctx, "google-pubsub-emulator", cfg.OTELEndpoint)
	if err != nil {
		slog.Error("failed to setup tracing", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			slog.Error("failed to shutdown tracing", "err", err)
		}
	}()

	// --- Storage: one shared State, one repo instance per type ---
	state := &memory.State{}
	topics := repositories.NewTopicRepository(state)
	subscriptions := repositories.NewSubscriptionRepository(state)
	snapshots := repositories.NewSnapshotRepository(state)
	messages := repositories.NewMessageRepository(state)
	pendingMessages := repositories.NewPendingMessageRepository(state)

	// --- Use cases ---
	topicUC := usecases.NewTopicUsecase(topics, subscriptions, snapshots)
	pubUC := usecases.NewPublisher(topics, messages, subscriptions, pendingMessages)
	subUC := usecases.NewSubscriber(ctx, topics, subscriptions, pendingMessages, messages, cfg.DeliveryDelay)
	snapUC := usecases.NewSnapshotUsecase(snapshots, subscriptions, messages, pendingMessages)

	// --- Optional init config: pre-create topics and subscriptions on startup ---
	if initPath := cfg.InitConfigPath; initPath != "" {
		initCfg, err := bootstrap.Load(initPath)
		if err != nil {
			slog.Error("failed to load init config", "path", initPath, "err", err)
			os.Exit(1)
		}
		if err := bootstrap.Apply(ctx, initCfg, topicUC, subUC); err != nil {
			slog.Error("failed to apply init config", "err", err)
			os.Exit(1)
		}
		slog.Info("init config applied", "path", initPath)
	}

	// --- Handlers ---
	pub := handler.NewPublisher(ctx, topicUC, pubUC)
	sub := handler.NewSubscriber(ctx, subUC, snapUC)

	otelInterceptor, err := otelconnect.NewInterceptor()
	if err != nil {
		slog.Error("failed to create otel interceptor", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()

	// --- Health endpoints ---
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	connectOpts := connect.WithInterceptors(otelInterceptor)
	mux.Handle(pubsubpbconnect.NewPublisherHandler(pub, connectOpts))
	mux.Handle(pubsubpbconnect.NewSubscriberHandler(sub, connectOpts))

	reflector := grpcreflect.NewStaticReflector(
		pubsubpbconnect.PublisherName,
		pubsubpbconnect.SubscriberName,
	)
	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))

	// --- Start server ---
	srv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	go func() {
		slog.Info("starting pubsub emulator", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// --- Graceful shutdown on SIGTERM / SIGINT ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "err", err)
		os.Exit(1)
	}
}
