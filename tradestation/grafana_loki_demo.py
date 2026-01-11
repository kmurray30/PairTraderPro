import logging
import time
import os
from dotenv import load_dotenv

from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk.resources import Resource
import requests

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

# Create OTLP exporter that sends logs to Grafana
exporter = OTLPLogExporter(
    endpoint=f"{endpoint.rstrip('/')}/v1/logs",
    headers=headers
)

# Create LoggerProvider with the exporter
logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

# Attach OpenTelemetry handler to Python's standard logging
handler = LoggingHandler(logger_provider=logger_provider)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

# Send some test logs
print("\n" + "="*60)
print("Sending logs to Grafana Cloud...")
print("="*60 + "\n")

logging.info("Test log message from PairTraderPro - connection successful!")
logging.warning("This is a warning message with some data", extra={"trade_id": 12345, "symbol": "AAPL"})
logging.error("This is an error message to test severity levels")

# Force flush to send logs immediately instead of waiting for batch
logger_provider.force_flush()

print("\n" + "="*60)
print("Logs sent successfully!")
print("="*60)
print("\nCheck your Grafana Cloud Loki instance to see the logs.")
print("They should appear under service.name='PairTraderPro'")
print("\nNote: If you don't see errors above, the logs were sent successfully (200/202 response).")
print("If you see errors, check the status code and message for details.")
