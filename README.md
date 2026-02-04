# Dirty Data Engine

Telemetry data producer that publishes structured JSON messages to RabbitMQ and
exposes endpoints for schema, samples, and publish control.

## Quick start

1. `docker compose up --build`
2. Sample payload: `http://localhost:18081/vitals/sample`
3. Schema: `http://localhost:18081/vitals/schema`
4. Health check: `http://localhost:18081/healthz`
5. Start publishing: `POST http://localhost:18081/vitals/publish/start`
6. Consume from RabbitMQ queue `telemetry_raw`

## HTTP API

- `POST /vitals/publish/start`  
  Starts publishing to RabbitMQ in the background.
- `POST /vitals/publish/stop`  
  Stops background publishing if running.
- `GET /vitals/publish/status`  
  Returns `{"running":true|false}`.
- `GET /vitals/sample`  
  Returns the deterministic first message.
- `GET /vitals/schema`  
  JSON Schema for the payload (`telemetry_schema.json`).
- `GET /healthz`  
  `{"ok":true}`

## Notes for consumers

- Messages are ~50KB; first message is deterministic.
- No auth/TLS is enabled by default.