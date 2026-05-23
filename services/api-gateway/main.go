package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	tracer trace.Tracer
	logger *slog.Logger
)

func initLogger() {
	// Create JSON handler for structured logging
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Add service name to all log entries
			if a.Key == slog.SourceKey {
				return slog.Attr{}
			}
			return a
		},
	})

	logger = slog.New(handler).With(
		slog.String("service", os.Getenv("SERVICE_NAME")),
	)

	slog.SetDefault(logger)
}

func initTracer() func() {
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "api-gateway"
	}

	tempoHostname := os.Getenv("TEMPO_HOSTNAME")
	if tempoHostname == "" {
		tempoHostname = "tempo"
	}

	tempoPort := os.Getenv("TEMPO_PORT")
	if tempoPort == "" {
		tempoPort = "4317"
	}

	endpoint := fmt.Sprintf("%s:%s", tempoHostname, tempoPort)

	ctx := context.Background()

	// Create OTLP trace exporter
	conn, err := grpc.DialContext(ctx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		logger.Error("failed to create gRPC connection to collector",
			slog.String("error", err.Error()),
			slog.String("endpoint", endpoint),
		)
		os.Exit(1)
	}

	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		logger.Error("failed to create trace exporter",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	// Create resource
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		logger.Error("failed to create resource",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	// Create trace provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer = tp.Tracer(serviceName)

	logger.Info("tracer initialized successfully",
		slog.String("service", serviceName),
		slog.String("endpoint", endpoint),
	)

	return func() {
		if err := tp.Shutdown(ctx); err != nil {
			logger.Error("error shutting down tracer provider",
				slog.String("error", err.Error()),
			)
		}
	}
}

type OrderRequest struct {
	Items           []OrderItem `json:"items" binding:"required"`
	PaymentMethod   string      `json:"payment_method" binding:"required"`
	Amount          interface{} `json:"amount" binding:"required"` // Can be string or float
	UserID          string      `json:"user_id" binding:"required"`
	ShippingAddress string      `json:"shipping_address,omitempty"`
}

type OrderItem struct {
	ItemID   string `json:"item_id" binding:"required"`
	Quantity int    `json:"quantity" binding:"required"`
}

// Helper function to convert amount to float64
func (o *OrderRequest) GetAmount() (float64, error) {
	switch v := o.Amount.(type) {
	case float64:
		return v, nil
	case string:
		var amount float64
		_, err := fmt.Sscanf(v, "%f", &amount)
		return amount, err
	case int:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("invalid amount type: %T", v)
	}
}

type OrderResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	TraceID string `json:"trace_id,omitempty"`
}

// Gin middleware for slog
func slogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get trace ID from context if available
		span := trace.SpanFromContext(c.Request.Context())
		traceID := span.SpanContext().TraceID().String()

		logger.Info("incoming request",
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.String("client_ip", c.ClientIP()),
			slog.String("trace_id", traceID),
		)

		c.Next()

		logger.Info("request completed",
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.Int("status", c.Writer.Status()),
			slog.String("trace_id", traceID),
		)
	}
}

func createOrder(c *gin.Context) {
	ctx := c.Request.Context()

	var req OrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.Error("failed to bind JSON",
			slog.String("error", err.Error()),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Get amount as float64
	amount, err := req.GetAmount()
	if err != nil {
		logger.Error("invalid amount format",
			slog.String("error", err.Error()),
			slog.Any("amount", req.Amount),
		)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid amount format"})
		return
	}

	logger.Debug("api-gateway received post request",
		slog.String("user_id", req.UserID),
		slog.Float64("amount", amount),
		slog.String("payment_method", req.PaymentMethod),
	)

	// Start a span for the request to order service
	ctx, span := tracer.Start(ctx, "request_to_order_service")
	defer span.End()

	// Get trace ID
	spanContext := span.SpanContext()
	traceID := spanContext.TraceID().String()

	span.SetAttributes(
		attribute.String("user_id", req.UserID),
		attribute.Float64("amount", amount),
		attribute.String("payment_method", req.PaymentMethod),
	)

	logger.Info("api-gateway makes a request to order-service",
		slog.String("trace_id", traceID),
		slog.String("user_id", req.UserID),
	)

	// Marshal request body
	jsonData, err := json.Marshal(req)
	if err != nil {
		logger.Error("error marshaling request",
			slog.String("error", err.Error()),
			slog.String("trace_id", traceID),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process request"})
		return
	}

	// Create HTTP request with context propagation
	httpReq, err := http.NewRequestWithContext(ctx, "POST", "http://order-service:5000/order", bytes.NewBuffer(jsonData))
	if err != nil {
		logger.Error("error creating request",
			slog.String("error", err.Error()),
			slog.String("trace_id", traceID),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create request"})
		return
	}

	httpReq.Header.Set("Content-Type", "application/json")

	// Propagate trace context
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))

	// Make the request
	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		logger.Error("error calling order service",
			slog.String("error", err.Error()),
			slog.String("trace_id", traceID),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to call order service"})
		return
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("error reading response",
			slog.String("error", err.Error()),
			slog.String("trace_id", traceID),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read response"})
		return
	}

	if resp.StatusCode != http.StatusOK {
		logger.Error("order service returned error",
			slog.String("response", string(body)),
			slog.Int("status_code", resp.StatusCode),
			slog.String("trace_id", traceID),
		)
	}

	// Parse and return response
	var orderResp OrderResponse
	if err := json.Unmarshal(body, &orderResp); err != nil {
		logger.Error("error parsing response",
			slog.String("error", err.Error()),
			slog.String("trace_id", traceID),
		)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse response"})
		return
	}

	logger.Info("order created successfully",
		slog.String("trace_id", traceID),
		slog.String("status", orderResp.Status),
	)

	c.JSON(http.StatusOK, orderResp)
}

func main() {
	// Initialize logger first
	initLogger()

	logger.Info("starting api-gateway service")

	// Initialize tracer
	cleanup := initTracer()
	defer cleanup()

	// Set Gin to release mode for production
	gin.SetMode(gin.ReleaseMode)

	// Create Gin router
	r := gin.New()

	// Add recovery middleware
	r.Use(gin.Recovery())

	// Add slog middleware
	r.Use(slogMiddleware())

	// Add OpenTelemetry middleware
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "api-gateway"
	}
	r.Use(otelgin.Middleware(serviceName))

	// Routes
	r.POST("/api/order", createOrder)

	// Health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "5000"
	}

	logger.Info("api-gateway server starting",
		slog.String("port", port),
		slog.String("service", serviceName),
	)

	if err := r.Run(":" + port); err != nil {
		logger.Error("failed to start server",
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}
}

// Made with Bob
