package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultPort          = "8080"
	defaultQueue         = "telemetry_raw"
	defaultRabbitDocker  = "amqp://guest:guest@rabbitmq:5672/"
	defaultRabbitLocal   = "amqp://guest:guest@localhost:5672/"
	normalInterval       = 2 * time.Second
	targetPayloadBytes   = 50 * 1024
	minStormMessagesPerS = 50
	maxStormMessagesPerS = 100
)

type TelemetryMessage struct {
	SchemaVersion string        `json:"schema_version"`
	Sequence      int64         `json:"sequence"`
	EmittedAtUnix int64         `json:"emitted_at_unix"` // deterministic timeline; 0 for sample
	Source        SourceInfo    `json:"source"`
	Payload       Payload       `json:"payload"`
	Integrity     IntegrityInfo `json:"integrity"`
}

type SourceInfo struct {
	System        string `json:"system"`
	Facility      string `json:"facility"`
	DeviceClass   string `json:"device_class"`
	DeviceID      string `json:"device_id"`
	Firmware      string `json:"firmware"`
	Protocol      string `json:"protocol"`
	ProtocolMinor int    `json:"protocol_minor"`
}

type Payload struct {
	Sensors []Sensor `json:"sensors"`
	Noise   Noise    `json:"noise"`
}

type Sensor struct {
	SensorID      string     `json:"sensor_id"`
	SensorType    string     `json:"sensor_type"`
	Hardware      HWInfo     `json:"hardware"`
	Calibration   CalInfo    `json:"calibration"`
	Data          SensorData `json:"data"`
	Diagnostics   DiagInfo   `json:"diagnostics"`
	RawWaveChunks []string   `json:"raw_wave_chunks"`
}

type HWInfo struct {
	Model          string `json:"model"`
	Serial         string `json:"serial"`
	Lot            string `json:"lot"`
	ManufacturedAt string `json:"manufactured_at"`
}

type CalInfo struct {
	Algorithm string             `json:"algorithm"`
	History   []CalibrationEvent `json:"history"`
}

type CalibrationEvent struct {
	TS   int64   `json:"ts"`
	Kind string  `json:"kind"`
	Gain float64 `json:"gain"`
	Bias float64 `json:"bias"`
	Note string  `json:"note"`
}

type SensorData struct {
	Metrics Metrics `json:"metrics"`
}

type Metrics struct {
	Vitals Vitals `json:"vitals"`
	Other  Other  `json:"other"`
}

// Critical workshop vitals: payload.sensors[0].data.metrics.vitals.{heart_rate,spo2,bp_systolic}
type Vitals struct {
	HeartRate int `json:"heart_rate"`
	Spo2      int `json:"spo2"`
	BPSys     int `json:"bp_systolic"`
}

type Other struct {
	RespRate       int     `json:"resp_rate"`
	TempC          float64 `json:"temp_c"`
	PerfusionIndex float64 `json:"perfusion_index"`
	SignalQuality  float64 `json:"signal_quality"`
}

type DiagInfo struct {
	HealthScore float64       `json:"health_score"`
	Flags       []string      `json:"flags"`
	HWLogs      []HardwareLog `json:"hardware_logs"`
}

type HardwareLog struct {
	TS       int64  `json:"ts"`
	Level    string `json:"level"`
	Module   string `json:"module"`
	Code     string `json:"code"`
	Message  string `json:"message"`
	TraceB64 string `json:"trace_b64"`
}

type Noise struct {
	EncryptedMetadata []string          `json:"encrypted_metadata"`
	BlobRefs          []string          `json:"blob_refs"`
	AuditTrail        []AuditEvent      `json:"audit_trail"`
	NoiseMatrix       [][]int           `json:"noise_matrix"`
	KeyValueJunk      map[string]string `json:"key_value_junk"`
	Padding           string            `json:"padding"`
}

type AuditEvent struct {
	TS        int64  `json:"ts"`
	Actor     string `json:"actor"`
	Action    string `json:"action"`
	ObjectRef string `json:"object_ref"`
	Details   string `json:"details"`
}

type IntegrityInfo struct {
	PayloadSHA256 string `json:"payload_sha256"`
	SignatureB64  string `json:"signature_b64"`
}

type Server struct {
	baseSeed   [32]byte
	rng        *rand.Rand // used for storm rate selection (not payload content)
	publisher  *RabbitPublisher
	queueName  string
	sampleJSON []byte
	schemaJSON []byte

	publishMu      sync.Mutex
	publishCancel  context.CancelFunc
	publishRunning bool
	publishSeq     int64
}

func main() {
	printSample := flag.Bool("print-sample", false, "print the deterministic first streamed JSON object and exit")
	writeSample := flag.Bool("write-sample", false, "write sample_telemetry.json (UTF-8 bytes) matching the first streamed object and exit")
	samplePath := flag.String("sample-path", "sample_telemetry.json", "path to write sample telemetry JSON when -write-sample is set")
	schemaPath := flag.String("schema-path", "telemetry_schema.json", "path to telemetry JSON schema for /vitals/schema")
	addr := flag.String("addr", "", "listen address (overrides PORT), e.g. :8080")
	flag.Parse()

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = defaultPort
	}
	listenAddr := ":" + port
	if *addr != "" {
		listenAddr = *addr
	}

	seedMaterial := strings.TrimSpace(os.Getenv("ENGINE_SEED"))
	if seedMaterial == "" {
		seedMaterial = "dirty-data-engine|workshop|seed|v1"
	}
	baseSeed := sha256.Sum256([]byte(seedMaterial))
	srv := NewServer(baseSeed)

	// RabbitMQ publisher: auto-configured with sensible defaults.
	queue := strings.TrimSpace(os.Getenv("RABBITMQ_QUEUE"))
	if queue == "" {
		queue = defaultQueue
	}
	srv.queueName = queue
	if pub, url := initRabbitPublisher(queue, 30*time.Second); pub != nil {
		log.Printf("rabbitmq enabled: url=%s queue=%s", url, queue)
		srv.publisher = pub
		defer pub.Close()
	} else {
		log.Printf("rabbitmq disabled: no broker reachable")
	}

	// Precompute deterministic first message (sequence 0) for sample_telemetry.json and first streamed chunk.
	srv.sampleJSON = srv.BuildTelemetryJSON(0, false)
	srv.schemaJSON = loadSchemaBytes(*schemaPath)

	if *writeSample {
		if err := os.WriteFile(*samplePath, srv.sampleJSON, 0644); err != nil {
			log.Fatalf("write sample: %v", err)
		}
		log.Printf("wrote sample: %s (%d bytes)", *samplePath, len(srv.sampleJSON))
		return
	}

	if *printSample {
		os.Stdout.Write(srv.sampleJSON)
		os.Stdout.Write([]byte("\n"))
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/vitals/sample", srv.handleSample)
	mux.HandleFunc("/vitals/schema", srv.handleSchema)
	mux.HandleFunc("/vitals/publish/start", srv.handlePublishStart)
	mux.HandleFunc("/vitals/publish/stop", srv.handlePublishStop)
	mux.HandleFunc("/vitals/publish/status", srv.handlePublishStatus)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	httpSrv := &http.Server{
		Addr:              listenAddr,
		Handler:           withLogging(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("listening on %s", listenAddr)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func NewServer(baseSeed [32]byte) *Server {
	seed64 := int64(binarySeed(baseSeed))
	return &Server{
		baseSeed:  baseSeed,
		rng:       rand.New(rand.NewSource(seed64)),
		queueName: defaultQueue,
	}
}

func (s *Server) handleSample(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if len(s.sampleJSON) == 0 {
		http.Error(w, "sample not available", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(s.sampleJSON)
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if len(s.schemaJSON) == 0 {
		http.Error(w, "schema not available", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/schema+json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(s.schemaJSON)
}

func (s *Server) handlePublishStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.publisher == nil {
		http.Error(w, "rabbitmq not configured", http.StatusServiceUnavailable)
		return
	}
	s.publishMu.Lock()
	if s.publishRunning {
		s.publishMu.Unlock()
		http.Error(w, "publisher already running", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.publishCancel = cancel
	s.publishRunning = true
	s.publishMu.Unlock()

	go s.runPublishLoop(ctx)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"ok":true,"running":true}`))
}

func (s *Server) handlePublishStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.publishMu.Lock()
	cancel := s.publishCancel
	running := s.publishRunning
	s.publishMu.Unlock()

	if cancel != nil {
		cancel()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if running {
		_, _ = w.Write([]byte(`{"ok":true,"running":false}`))
	} else {
		_, _ = w.Write([]byte(`{"ok":true,"running":false,"note":"not running"}`))
	}
}

func (s *Server) handlePublishStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.publishMu.Lock()
	running := s.publishRunning
	s.publishMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if running {
		_, _ = w.Write([]byte(`{"running":true}`))
	} else {
		_, _ = w.Write([]byte(`{"running":false}`))
	}
}

func (s *Server) runPublishLoop(ctx context.Context) {
	defer func() {
		s.publishMu.Lock()
		s.publishRunning = false
		s.publishCancel = nil
		s.publishMu.Unlock()
	}()

	log.Printf("publisher started: rate=normal with periodic spikes")

	const (
		spikeEvery = 8 * time.Second
		spikeFor   = 2 * time.Second
	)
	nextSpike := time.Now().Add(spikeEvery)
	spikeUntil := time.Time{}
	interval := normalInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("publisher stopped")
			return
		case <-ticker.C:
			now := time.Now()
			if spikeUntil.IsZero() && now.After(nextSpike) {
				rps := s.pickStormRPS()
				interval = time.Second / time.Duration(rps)
				spikeUntil = now.Add(spikeFor)
				nextSpike = now.Add(spikeEvery)
				ticker.Reset(interval)
				log.Printf("publisher spike: rps=%d for=%s", rps, spikeFor)
			} else if !spikeUntil.IsZero() && now.After(spikeUntil) {
				spikeUntil = time.Time{}
				interval = normalInterval
				ticker.Reset(interval)
				log.Printf("publisher spike ended; rate=normal")
			}

			seq := atomic.AddInt64(&s.publishSeq, 1) - 1
			msg := s.BuildTelemetryJSON(seq, true)
			if s.publisher != nil {
				s.publisher.TryPublish(msg)
			}
		}
	}
}

func (s *Server) pickStormRPS() int {
	return minStormMessagesPerS + s.rng.Intn(maxStormMessagesPerS-minStormMessagesPerS+1)
}

// BuildTelemetryJSON creates a marshaled JSON object approximately targetPayloadBytes bytes.
// If vary=true, it mutates some non-critical fields based on sequence.
func (s *Server) BuildTelemetryJSON(sequence int64, vary bool) []byte {
	msg := s.buildTelemetryObject(sequence, vary)
	b, _ := json.Marshal(&msg)
	return s.padToTarget(&msg, b, targetPayloadBytes)
}

func (s *Server) buildTelemetryObject(sequence int64, vary bool) TelemetryMessage {
	seed := sha256.Sum256([]byte(fmt.Sprintf("%x|seq=%d", s.baseSeed, sequence)))
	prng := rand.New(rand.NewSource(int64(binarySeed(seed))))

	// Noise sizing knobs. Keep the payload heavily nested + noisy, but leave room for Padding
	// to bring the final JSON size to ~50KB.
	const (
		encryptedMetaCount = 6
		blobRefCount       = 12
		auditCount         = 8
		noiseMatrixN       = 24
		keyValueJunkCount  = 20
		hwLogCount         = 15
		calHistoryCount    = 12
		rawWaveCount       = 10
	)

	emittedAt := int64(0)
	if vary {
		emittedAt = 1700000000 + sequence
	}

	v := Vitals{
		HeartRate: 60 + int(sequence%40),
		Spo2:      92 + int(sequence%8),
		BPSys:     110 + int(sequence%25),
	}
	if !vary && sequence == 0 {
		v = Vitals{HeartRate: 78, Spo2: 97, BPSys: 124}
	}

	noiseMeta := make([]string, 0, encryptedMetaCount)
	for i := 0; i < encryptedMetaCount; i++ {
		noiseMeta = append(noiseMeta, fakeEncryptedString(prng, 420+(i*5)))
	}

	blobRefs := make([]string, 0, blobRefCount)
	for i := 0; i < blobRefCount; i++ {
		blobRefs = append(blobRefs, fmt.Sprintf("blob://%s/%s/%08x",
			pick(prng, "vaultA", "vaultB", "vaultC"),
			pick(prng, "raw", "diag", "meta"),
			prng.Uint32(),
		))
	}

	audit := make([]AuditEvent, 0, auditCount)
	for i := 0; i < auditCount; i++ {
		audit = append(audit, AuditEvent{
			TS:        1699995000 + int64(i)*37 + sequence,
			Actor:     pick(prng, "svc_ingest", "svc_router", "svc_normalizer", "svc_export"),
			Action:    pick(prng, "UPSERT", "ENRICH", "VALIDATE", "ROUTE", "DROP_SHADOW"),
			ObjectRef: fmt.Sprintf("ref:%08x:%04x", prng.Uint32(), prng.Uint32()%65536),
			Details:   fakeEncryptedString(prng, 260),
		})
	}

	matrix := make([][]int, noiseMatrixN)
	for i := range matrix {
		row := make([]int, noiseMatrixN)
		for j := range row {
			row[j] = int(prng.Int31() % 1024)
		}
		matrix[i] = row
	}

	junk := map[string]string{}
	for i := 0; i < keyValueJunkCount; i++ {
		k := fmt.Sprintf("k_%02d_%s", i, shortHex(seed[:], i))
		junk[k] = fakeEncryptedString(prng, 64+(i%7)*5)
	}

	hwLogs := make([]HardwareLog, 0, hwLogCount)
	for i := 0; i < hwLogCount; i++ {
		hwLogs = append(hwLogs, HardwareLog{
			TS:       1699990000 + int64(i)*11 + sequence,
			Level:    pick(prng, "DEBUG", "INFO", "WARN", "ERROR"),
			Module:   pick(prng, "adc", "ble", "battery", "storage", "rtos", "crypto"),
			Code:     fmt.Sprintf("E%04d", prng.Intn(9999)),
			Message:  pick(prng, "buffer underrun", "checksum mismatch", "retrying frame", "cal drift", "nonce reuse suspected", "uart framing"),
			TraceB64: fakeTraceB64(prng, 220),
		})
	}

	calHist := make([]CalibrationEvent, 0, calHistoryCount)
	for i := 0; i < calHistoryCount; i++ {
		calHist = append(calHist, CalibrationEvent{
			TS:   1690000000 + int64(i)*86400 + (sequence % 100),
			Kind: pick(prng, "factory", "field", "self_test", "recal"),
			Gain: 0.9 + prng.Float64()*0.2,
			Bias: -0.05 + prng.Float64()*0.1,
			Note: fakeEncryptedString(prng, 180),
		})
	}

	rawWaves := make([]string, 0, rawWaveCount)
	for i := 0; i < rawWaveCount; i++ {
		rawWaves = append(rawWaves, fakeEncryptedString(prng, 520))
	}

	// Avoid duplicating the biggest noisy arrays across all sensors; keep extra sensors smaller.
	calHist2 := calHist
	if len(calHist2) > 4 {
		calHist2 = calHist2[:4]
	}
	hwLogs2 := hwLogs
	if len(hwLogs2) > 3 {
		hwLogs2 = hwLogs2[:3]
	}
	rawWaves2 := rawWaves
	if len(rawWaves2) > 2 {
		rawWaves2 = rawWaves2[:2]
	}

	sensors := []Sensor{
		{
			SensorID:   "SEN-0001",
			SensorType: "pulse_oximeter",
			Hardware: HWInfo{
				Model:          "MDT-PULSOX-7",
				Serial:         "PX7-4F2A-19C0",
				Lot:            "LOT-2026-02-A",
				ManufacturedAt: "2026-01-15",
			},
			Calibration: CalInfo{
				Algorithm: "kalman+polyfit-v3",
				History:   calHist,
			},
			Data: SensorData{
				Metrics: Metrics{
					Vitals: v,
					Other: Other{
						RespRate:       12 + int(sequence%10),
						TempC:          36.4 + float64(sequence%10)*0.03,
						PerfusionIndex: 0.8 + prng.Float64()*2.2,
						SignalQuality:  0.2 + prng.Float64()*0.8,
					},
				},
			},
			Diagnostics: DiagInfo{
				HealthScore: 0.4 + prng.Float64()*0.6,
				Flags: []string{
					pick(prng, "OK", "LOW_SIGNAL", "MOTION", "CAL_DRIFT", "TEMP_COMP", "BATT_LOW"),
					pick(prng, "OK", "OK", "OK", "FRAME_RETRY", "CRC_WARN"),
				},
				HWLogs: hwLogs,
			},
			RawWaveChunks: rawWaves,
		},
		{
			SensorID:   "SEN-0002",
			SensorType: "pressure_cuff",
			Hardware: HWInfo{
				Model:          "MDT-CUFF-2",
				Serial:         "CF2-0A9B-77D1",
				Lot:            "LOT-2025-11-B",
				ManufacturedAt: "2025-10-22",
			},
			Calibration: CalInfo{
				Algorithm: "offset+tempcomp-v2",
				History:   calHist2,
			},
			Data: SensorData{
				Metrics: Metrics{
					Vitals: Vitals{},
					Other:  Other{},
				},
			},
			Diagnostics: DiagInfo{
				HealthScore: 0.3 + prng.Float64()*0.6,
				Flags:       []string{pick(prng, "OK", "PUMP_WARN", "VALVE_STICKY", "NOISE_SPIKE")},
				HWLogs:      hwLogs2,
			},
			RawWaveChunks: rawWaves2,
		},
	}

	payload := Payload{
		Sensors: sensors,
		Noise: Noise{
			EncryptedMetadata: noiseMeta,
			BlobRefs:          blobRefs,
			AuditTrail:        audit,
			NoiseMatrix:       matrix,
			KeyValueJunk:      junk,
			Padding:           "",
		},
	}

	payloadBytes, _ := json.Marshal(payload)
	sum := sha256.Sum256(payloadBytes)
	sig := base64.RawStdEncoding.EncodeToString(sum[:])
	if len(sig) > 128 {
		sig = sig[:128]
	}

	return TelemetryMessage{
		SchemaVersion: "telemetry.dirty.v1",
		Sequence:      sequence,
		EmittedAtUnix: emittedAt,
		Source: SourceInfo{
			System:        "medical_telemetry_workshop",
			Facility:      "lab_sim",
			DeviceClass:   "telemetry_bridge",
			DeviceID:      "bridge-01",
			Firmware:      "v0.1.0-workshop",
			Protocol:      "mdt-telemetry",
			ProtocolMinor: 7,
		},
		Payload: payload,
		Integrity: IntegrityInfo{
			PayloadSHA256: fmt.Sprintf("%x", sum[:]),
			SignatureB64:  sig,
		},
	}
}

func (s *Server) padToTarget(msg *TelemetryMessage, initial []byte, target int) []byte {
	b := initial
	// If we're already near target, keep it.
	if len(b) >= target-1024 && len(b) <= target+1024 {
		return b
	}

	// If we're over target (likely due to other noisy fields), do not attempt to shrink
	// by modifying padding (it can only grow). Keep as-is.
	// Workshop requirement says "approximately 50KB"; we'll keep noise heavy but allow
	// tuning by reducing default noise counts if needed.
	if len(b) > target+1024 {
		return b
	}

	base := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	need := target - len(b)
	if need < 0 {
		need = 0
	}

	padLen := need + 256
	for iter := 0; iter < 8; iter++ {
		msg.Payload.Noise.Padding = repeatPattern(base, padLen)
		nb, _ := json.Marshal(msg)
		diff := target - len(nb)
		if diff >= -512 && diff <= 512 {
			return nb
		}
		padLen += diff
		if padLen < 0 {
			padLen = 0
		}
		b = nb
	}
	return b
}

func repeatPattern(pattern string, n int) string {
	if n <= 0 {
		return ""
	}
	var sb strings.Builder
	sb.Grow(n)
	for sb.Len() < n {
		remain := n - sb.Len()
		if remain >= len(pattern) {
			sb.WriteString(pattern)
		} else {
			sb.WriteString(pattern[:remain])
		}
	}
	return sb.String()
}

func fakeEncryptedString(r *rand.Rand, approxLen int) string {
	if approxLen < 16 {
		approxLen = 16
	}
	raw := make([]byte, (approxLen*3)/4+8)
	for i := range raw {
		raw[i] = byte(r.Intn(256))
	}
	s := base64.RawURLEncoding.EncodeToString(raw)
	if len(s) > approxLen {
		s = s[:approxLen]
	}
	return s
}

func fakeTraceB64(r *rand.Rand, approxLen int) string {
	// Different alphabet to simulate compressed traces.
	b := fakeEncryptedString(r, approxLen)
	return strings.ReplaceAll(b, "-", "+")
}

func shortHex(b []byte, salt int) string {
	h := sha256.Sum256(append(b, byte(salt)))
	return fmt.Sprintf("%x", h[:])[:10]
}

func binarySeed(seed [32]byte) uint64 {
	var v uint64
	for i := 0; i < 8; i++ {
		v = (v << 8) | uint64(seed[i])
	}
	return v
}

func pick(r *rand.Rand, options ...string) string {
	return options[r.Intn(len(options))]
}

// RabbitPublisher publishes JSON bytes to an AMQP queue.
type RabbitPublisher struct {
	queue string
	ch    *amqp.Channel
	conn  *amqp.Connection

	publishCh chan []byte
	done      chan struct{}
}

func NewRabbitPublisher(url, queue string) (*RabbitPublisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	p := &RabbitPublisher{
		queue:     queue,
		ch:        ch,
		conn:      conn,
		publishCh: make(chan []byte, 5000),
		done:      make(chan struct{}),
	}
	go p.loop()
	return p, nil
}

func (p *RabbitPublisher) TryPublish(body []byte) {
	cp := make([]byte, len(body))
	copy(cp, body)
	select {
	case p.publishCh <- cp:
	default:
		// Drop if storm mode outruns RabbitMQ.
	}
}

func (p *RabbitPublisher) loop() {
	for {
		select {
		case <-p.done:
			return
		case body := <-p.publishCh:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = p.ch.PublishWithContext(ctx, "", p.queue, false, false, amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Unix(0, 0),
				Body:         body,
			})
			cancel()
		}
	}
}

func (p *RabbitPublisher) Close() {
	close(p.done)
	// drain best-effort
	for {
		select {
		case <-p.publishCh:
		default:
			goto drained
		}
	}
drained:
	if p.ch != nil {
		_ = p.ch.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s %s", r.Method, r.URL.Path, r.RemoteAddr, time.Since(start))
	})
}

func loadSchemaBytes(path string) []byte {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		log.Printf("schema unavailable (%s): %v", path, err)
		return nil
	}
	return b
}

func initRabbitPublisher(queue string, maxWait time.Duration) (*RabbitPublisher, string) {
	if url := strings.TrimSpace(os.Getenv("RABBITMQ_URL")); url != "" {
		if pub, ok := retryConnect(url, queue, maxWait); ok {
			return pub, url
		}
		return nil, ""
	}

	candidates := []string{defaultRabbitDocker, defaultRabbitLocal}
	for _, url := range candidates {
		if pub, ok := retryConnect(url, queue, maxWait); ok {
			return pub, url
		}
	}
	return nil, ""
}

func retryConnect(url, queue string, maxWait time.Duration) (*RabbitPublisher, bool) {
	deadline := time.Now().Add(maxWait)
	delay := 500 * time.Millisecond
	for {
		pub, err := NewRabbitPublisher(url, queue)
		if err == nil {
			return pub, true
		}
		if time.Now().After(deadline) {
			log.Printf("rabbitmq connect failed (%s): %v", url, err)
			return nil, false
		}
		time.Sleep(delay)
		if delay < 3*time.Second {
			delay += 500 * time.Millisecond
		}
	}
}
