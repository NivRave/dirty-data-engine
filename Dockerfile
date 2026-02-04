FROM public.ecr.aws/docker/library/golang:1.22-alpine AS builder
WORKDIR /src
RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/dirty-data-engine ./main.go

FROM public.ecr.aws/docker/library/alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /out/dirty-data-engine /app/dirty-data-engine
COPY telemetry_schema.json /app/telemetry_schema.json

EXPOSE 8080
ENV PORT=8080
CMD ["/app/dirty-data-engine"]

FROM public.ecr.aws/docker/library/golang:1.22-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/dirty-data-engine .

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/dirty-data-engine /app/dirty-data-engine
COPY telemetry_schema.json /app/telemetry_schema.json

EXPOSE 8080
ENV PORT=8080

ENTRYPOINT ["/app/dirty-data-engine"]


