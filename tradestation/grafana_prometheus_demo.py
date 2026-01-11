import time
import os
from dotenv import load_dotenv

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# Load environment variables from .env file
load_dotenv()

# Parse environment variables for OTLP configuration
endpoint = os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"]
headers_raw = os.environ["OTEL_EXPORTER_OTLP_HEADERS"]

# Parse headers in format "key=value,key2=value2" into a dictionary
headers = {}
for part in headers_raw.split(","):
    if "=" in part:
        key, value = part.split("=", 1)
        headers[key.strip()] = value.strip()

print(f"Connecting to: {endpoint}")
print(f"Auth header present: {'Authorization' in headers}")

# Configure OpenTelemetry with a resource that identifies this application
resource = Resource.create({
    "service.name": "PairTraderPro",
    "service.version": "1.0.0",
    "deployment.environment": "development"
})

# Create OTLP exporter that sends metrics to Grafana Prometheus
metric_exporter = OTLPMetricExporter(
    endpoint=f"{endpoint.rstrip('/')}/v1/metrics",
    headers=headers
)

# Create MeterProvider with periodic export (every 10 seconds by default)
metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=5000)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])

# Get a meter to create instruments
meter = meter_provider.get_meter("PairTraderPro")

print("\n" + "="*60)
print("Sending metrics to Grafana Cloud Prometheus...")
print("="*60 + "\n")

# Create different types of metrics to demonstrate functionality

# 1. Counter - monotonically increasing value (e.g., total API calls)
api_calls_counter = meter.create_counter(
    name="api.calls.total",
    description="Total number of API calls made",
    unit="calls"
)

# 2. Histogram - measure distribution of values (e.g., API latency)
api_latency_histogram = meter.create_histogram(
    name="api.latency",
    description="API call latency distribution",
    unit="ms"
)

# 3. UpDownCounter - can go up or down (e.g., active connections)
active_connections = meter.create_up_down_counter(
    name="connections.active",
    description="Number of active connections",
    unit="connections"
)

# Record some example metrics
print("Recording example metrics...")

# Simulate API calls with different endpoints and response times
api_endpoints = [
    ("/v3/brokerage/accounts", 45.2),
    ("/v3/brokerage/orders", 123.5),
    ("/v3/brokerage/positions", 67.8),
    ("/v3/marketdata/quotes", 34.1),
    ("/v3/brokerage/balances", 89.3)
]

for endpoint_path, latency_ms in api_endpoints:
    # Increment API call counter
    api_calls_counter.add(1, {"endpoint": endpoint_path, "status": "success"})
    
    # Record latency
    api_latency_histogram.record(latency_ms, {"endpoint": endpoint_path})
    
    print(f"  Recorded: {endpoint_path} - {latency_ms}ms")

# Simulate some failed calls
api_calls_counter.add(2, {"endpoint": "/v3/brokerage/orders", "status": "error"})
print(f"  Recorded: 2 failed API calls")

# Track active connections going up and down
active_connections.add(5, {"connection_type": "websocket"})
active_connections.add(-2, {"connection_type": "websocket"})
print(f"  Recorded: Connection changes (net +3 websocket connections)")

# Add some gauge-like behavior with UpDownCounter
active_connections.add(10, {"connection_type": "http"})
print(f"  Recorded: 10 active HTTP connections")

print("\nMetrics recorded. Flushing to Grafana Cloud...")

# Force flush to send metrics immediately instead of waiting for next export interval
meter_provider.force_flush()

print("\n" + "="*60)
print("Metrics sent successfully!")
print("="*60)
print("\nCheck your Grafana Cloud Prometheus instance to see the metrics.")
print("They should appear under service.name='PairTraderPro'")
print("\nMetrics created:")
print("  - api.calls.total (Counter)")
print("  - api.latency (Histogram)")
print("  - connections.active (UpDownCounter)")
print("\nNote: If you don't see errors above, the metrics were sent successfully (200/202 response).")
print("If you see errors, check the status code and message for details.")

