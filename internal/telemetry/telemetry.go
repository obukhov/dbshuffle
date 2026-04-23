package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/credentials"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// otelLogHandler holds the otelslog bridge handler when log export is enabled,
// nil otherwise. Set by Setup and read by LogHandler.
var otelLogHandler slog.Handler

// LogHandler returns the OTel slog bridge handler to use alongside the normal
// text handler, or nil when log export is disabled.
func LogHandler() slog.Handler { return otelLogHandler }

// Setup initialises the global OTel TracerProvider, MeterProvider and
// LoggerProvider from env vars. All three signals are disabled by default
// and enabled by setting an OTLP endpoint or an explicit exporter name.
//
// Relevant env vars:
//
//	OTEL_TRACES_EXPORTER        otlp | stdout | none
//	OTEL_METRICS_EXPORTER       otlp | stdout | none  (defaults same as traces)
//	OTEL_LOGS_EXPORTER          otlp | stdout | none  (defaults same as traces)
//	OTEL_EXPORTER_OTLP_ENDPOINT host:port or URL  (auto-enables otlp for all signals)
//	OTEL_EXPORTER_OTLP_PROTOCOL grpc (default) | http/protobuf
//	OTEL_SERVICE_NAME           service name (default: dbshuffle)
//
// Returns a shutdown function that flushes and stops all providers.
func Setup(ctx context.Context) (func(context.Context) error, error) {
	slog.Debug("telemetry: env",
		"OTEL_TRACES_EXPORTER", os.Getenv("OTEL_TRACES_EXPORTER"),
		"OTEL_METRICS_EXPORTER", os.Getenv("OTEL_METRICS_EXPORTER"),
		"OTEL_LOGS_EXPORTER", os.Getenv("OTEL_LOGS_EXPORTER"),
		"OTEL_EXPORTER_OTLP_ENDPOINT", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		"OTEL_EXPORTER_OTLP_PROTOCOL", os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"),
		"OTEL_EXPORTER_OTLP_HEADERS", os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"),
	)
	traceExp := resolveExporterName("OTEL_TRACES_EXPORTER")
	metricExp := resolveExporterName("OTEL_METRICS_EXPORTER")
	logExp := resolveExporterName("OTEL_LOGS_EXPORTER")

	if traceExp == "none" && metricExp == "none" && logExp == "none" {
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	svcName := os.Getenv("OTEL_SERVICE_NAME")
	if svcName == "" {
		svcName = "dbshuffle"
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(svcName)),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, fmt.Errorf("create otel resource: %w", err)
	}

	var shutdowns []func(context.Context) error

	if traceExp != "none" {
		exp, err := newTraceExporter(ctx, traceExp)
		if err != nil {
			return nil, err
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
		shutdowns = append(shutdowns, tp.Shutdown)
	} else {
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
	}

	if metricExp != "none" {
		exp, err := newMetricExporter(ctx, metricExp)
		if err != nil {
			return nil, err
		}
		mp := metric.NewMeterProvider(
			metric.WithReader(metric.NewPeriodicReader(exp)),
			metric.WithResource(res),
		)
		otel.SetMeterProvider(mp)
		shutdowns = append(shutdowns, mp.Shutdown)
	}

	if logExp != "none" {
		exp, err := newLogExporter(ctx, logExp)
		if err != nil {
			return nil, err
		}
		lp := sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
			sdklog.WithResource(res),
		)
		shutdowns = append(shutdowns, lp.Shutdown)
		otelLogHandler = otelslog.NewHandler("dbshuffle", otelslog.WithLoggerProvider(lp))
	}

	return func(ctx context.Context) error {
		var errs []error
		for _, s := range shutdowns {
			sCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := s(sCtx); err != nil {
				errs = append(errs, err)
			}
			cancel()
		}
		return errors.Join(errs...)
	}, nil
}

func resolveExporterName(envVar string) string {
	if v := strings.ToLower(strings.TrimSpace(os.Getenv(envVar))); v != "" {
		return v
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		return "otlp"
	}
	return "none"
}

func isHTTP() bool {
	return strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"))) == "http/protobuf"
}

func otlpHeaders() map[string]string {
	raw := os.Getenv("OTEL_EXPORTER_OTLP_HEADERS")
	if raw == "" {
		return nil
	}
	headers := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if ok {
			headers[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return headers
}

func grpcOpts() []otlptracegrpc.Option {
	opts := []otlptracegrpc.Option{otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, ""))}
	if ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); ep != "" {
		opts = append(opts, otlptracegrpc.WithEndpoint(ep))
	}
	if h := otlpHeaders(); len(h) > 0 {
		opts = append(opts, otlptracegrpc.WithHeaders(h))
	}
	return opts
}

func newTraceExporter(ctx context.Context, name string) (sdktrace.SpanExporter, error) {
	switch name {
	case "stdout":
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	case "otlp":
		if isHTTP() {
			return otlptracehttp.New(ctx)
		}
		return otlptracegrpc.New(ctx, grpcOpts()...)
	default:
		return nil, fmt.Errorf("unsupported OTEL_TRACES_EXPORTER %q (valid: otlp, stdout, none)", name)
	}
}

func newMetricExporter(ctx context.Context, name string) (metric.Exporter, error) {
	switch name {
	case "stdout":
		return stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	case "otlp":
		if isHTTP() {
			return otlpmetrichttp.New(ctx)
		}
		opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, ""))}
		if ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); ep != "" {
			opts = append(opts, otlpmetricgrpc.WithEndpoint(ep))
		}
		if h := otlpHeaders(); len(h) > 0 {
			opts = append(opts, otlpmetricgrpc.WithHeaders(h))
		}
		return otlpmetricgrpc.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("unsupported OTEL_METRICS_EXPORTER %q (valid: otlp, stdout, none)", name)
	}
}

func newLogExporter(ctx context.Context, name string) (sdklog.Exporter, error) {
	switch name {
	case "stdout":
		return stdoutlog.New(stdoutlog.WithPrettyPrint())
	case "otlp":
		if isHTTP() {
			return otlploghttp.New(ctx)
		}
		opts := []otlploggrpc.Option{otlploggrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, ""))}
		if ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); ep != "" {
			opts = append(opts, otlploggrpc.WithEndpoint(ep))
		}
		if h := otlpHeaders(); len(h) > 0 {
			opts = append(opts, otlploggrpc.WithHeaders(h))
		}
		return otlploggrpc.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("unsupported OTEL_LOGS_EXPORTER %q (valid: otlp, stdout, none)", name)
	}
}
