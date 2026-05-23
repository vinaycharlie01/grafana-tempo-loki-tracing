package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.opentelemetry.io/otel/trace"
	_ "modernc.org/sqlite"
)

type config struct {
	tempoHost        string
	tempoPort        string
	fraudPercentage  int
	notFraudPercent  int
	serviceName      string
	databaseFilePath string
}

type fraudRequest struct {
	OrderID       stringOrNumber `json:"order_id"`
	UserID        stringOrNumber `json:"user_id"`
	PaymentMethod string         `json:"payment_method"`
	Amount        interface{}    `json:"amount"`
}

func main() {
	opts := &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	cfg := loadConfig(logger)
	slog.Info("config loaded",
		slog.String("service", cfg.serviceName),
		slog.String("tempo_host", cfg.tempoHost),
		slog.String("tempo_port", cfg.tempoPort),
		slog.Int("fraud_percentage", cfg.fraudPercentage),
		slog.Int("not_fraud_percentage", cfg.notFraudPercent),
		slog.String("db_path", cfg.databaseFilePath),
	)

	ctx := context.Background()
	tracerProvider, err := initTracer(ctx, cfg)
	if err != nil {
		slog.Error("failed to initialize tracing", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := tracerProvider.Shutdown(shutdownCtx); shutdownErr != nil {
			slog.Error("failed to shutdown tracer provider", slog.String("error", shutdownErr.Error()))
		}
	}()

	db, err := initDatabase(cfg.databaseFilePath)
	if err != nil {
		slog.Error("failed to initialize sqlite database", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer db.Close()
	slog.Info("sqlite database initialized", slog.String("path", cfg.databaseFilePath))

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(ginSlogMiddleware(logger))
	router.Use(otelgin.Middleware(cfg.serviceName))

	tracer := otel.Tracer(cfg.serviceName)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	router.POST("/fraud/check", func(c *gin.Context) {
		var payload fraudRequest
		if err := c.ShouldBindJSON(&payload); err != nil {
			slog.Warn("invalid request payload", slog.String("error", err.Error()))
			c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Invalid request payload"})
			return
		}

		ctx, span := tracer.Start(c.Request.Context(), "check_fraud")
		defer span.End()

		traceID, spanID := traceIDsFromContext(ctx)
		slog.Info("fraud-service logged trace_id",
			slog.String("trace_id", traceID),
		)
		slog.Info("fraud check received",
			slog.String("trace_id", traceID),
			slog.String("span_id", spanID),
			slog.String("order_id", string(payload.OrderID)),
			slog.String("user_id", string(payload.UserID)),
			slog.String("payment_method", payload.PaymentMethod),
			slog.String("amount", fmt.Sprint(payload.Amount)),
		)

		ctx, analyzeSpan := tracer.Start(ctx, "analyze_transaction")
		analyzeSpan.SetAttributes(
			attribute.String("fraud.order_id", string(payload.OrderID)),
			attribute.String("fraud.user_id", string(payload.UserID)),
			attribute.String("fraud.payment_method", payload.PaymentMethod),
			attribute.String("fraud.amount", fmt.Sprint(payload.Amount)),
		)

		isFraudulent := fraudProbability(rng, cfg.fraudPercentage, cfg.notFraudPercent)
		analyzeSpan.SetAttributes(attribute.Bool("fraud.is_fraudulent", isFraudulent))
		analyzeSpan.End()

		if isFraudulent {
			slog.Warn("transaction flagged as fraudulent",
				slog.String("trace_id", traceID),
				slog.String("span_id", spanID),
				slog.String("order_id", string(payload.OrderID)),
				slog.String("user_id", string(payload.UserID)),
				slog.String("payment_method", payload.PaymentMethod),
				slog.String("amount", fmt.Sprint(payload.Amount)),
			)
			c.JSON(http.StatusOK, gin.H{"status": "fraudulent", "message": "Transaction is fraudulent"})
			return
		}

		slog.Info("transaction passed fraud check",
			slog.String("trace_id", traceID),
			slog.String("span_id", spanID),
			slog.String("order_id", string(payload.OrderID)),
			slog.String("user_id", string(payload.UserID)),
			slog.String("payment_method", payload.PaymentMethod),
			slog.String("amount", fmt.Sprint(payload.Amount)),
		)
		c.JSON(http.StatusOK, gin.H{"status": "legitimate", "message": "Transaction is legitimate"})
	})

	server := &http.Server{
		Addr:    ":5000",
		Handler: router,
	}

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-shutdownCh
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("failed to shutdown server", slog.String("error", err.Error()))
		}
	}()

	slog.Info("fraud-service listening on :5000")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server exited with error", slog.String("error", err.Error()))
	}
}

func ginSlogMiddleware(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method
		c.Next()

		traceID, spanID := traceIDsFromContext(c.Request.Context())
		slog.Info("http request",
			slog.String("trace_id", traceID),
			slog.String("span_id", spanID),
			slog.String("method", method),
			slog.String("path", path),
			slog.Int("status", c.Writer.Status()),
			slog.String("client_ip", c.ClientIP()),
			slog.String("user_agent", c.Request.UserAgent()),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()),
		)
	}
}

func loadConfig(logger *slog.Logger) config {
	return config{
		tempoHost:        getenvDefault("TEMPO_HOSTNAME", "tempo"),
		tempoPort:        getenvDefault("TEMPO_PORT", "4317"),
		fraudPercentage:  getIntEnv("FRAUD_PERCENTAGE", 5, logger),
		notFraudPercent:  getIntEnv("NOT_FRAUD_PERCENTAGE", 95, logger),
		serviceName:      "fraud-service",
		databaseFilePath: "/sqlite.db",
	}
}

func getenvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getIntEnv(key string, fallback int, logger *slog.Logger) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		slog.Warn("invalid integer env var, using default", slog.String("key", key), slog.String("value", value))
		return fallback
	}
	return parsed
}

func initTracer(ctx context.Context, cfg config) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(fmt.Sprintf("%s:%s", cfg.tempoHost, cfg.tempoPort)),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	resource, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.serviceName)),
	)
	if err != nil {
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tracerProvider, nil
}

func initDatabase(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	createTable := `CREATE TABLE IF NOT EXISTS fraud_detecton (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		order_id TEXT,
		user_id TEXT,
		payment_method TEXT,
		amount TEXT,
		is_fraud INTEGER
	);`

	if _, err := db.Exec(createTable); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func fraudProbability(rng *rand.Rand, fraudWeight, notFraudWeight int) bool {
	total := fraudWeight + notFraudWeight
	if total <= 0 {
		return false
	}
	return rng.Intn(total) < fraudWeight
}

type stringOrNumber string

func (s *stringOrNumber) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if data[0] == '"' {
		var str string
		if err := json.Unmarshal(data, &str); err != nil {
			return err
		}
		*s = stringOrNumber(str)
		return nil
	}
	var num json.Number
	if err := json.Unmarshal(data, &num); err != nil {
		return err
	}
	*s = stringOrNumber(num.String())
	return nil
}

func traceIDsFromContext(ctx context.Context) (string, string) {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return "", ""
	}
	return spanContext.TraceID().String(), spanContext.SpanID().String()
}
