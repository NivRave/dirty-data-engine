# Mission: The "Vital Stream" Final Blueprint

## Mission Material: The Contract

You are given this producer service. Run it locally with Docker Compose from
this repository, and then build a **separate** project (your own backend +
frontend) that consumes its data.

Schema endpoint: `http://localhost:18081/vitals/schema`  
Sample payload: `http://localhost:18081/vitals/sample`

The schema confirms that the data is located at:

`payload -> sensors[] -> data -> metrics -> vitals`

## Mission Objectives & Tasks

### 1) The Extraction Protocol (Discovery)

Provide the raw `/vitals/sample` payload and the schema to your AI Agent.

**Task:**  
"Using the provided schema and sample, generate a Go struct that performs a
'Surgical Extraction'. We only want the top-level `sequence` and the `vitals`
object for every sensor in the array. Ignore all other fields."

**Guardrail:**  
If the Go struct contains a `noise` field or `raw_wave_chunks`, the task is
incomplete.

### 2) Resilient Stream Processing (Backend)

Implement the RabbitMQ consumer using the `pond` worker pool in your **new**
backend project.

**Spike Challenge:**  
The server spikes every once in a while

**Prompt:**  
"Implement a worker pool with 20 concurrent workers. Each worker should
unmarshal the ~50KB JSON, extract the vitals, and log them. If the unmarshaling
takes more than 5ms, log a warning."

**Success Metric:**  
During a spike, the sequence numbers in the logs should remain sequential and
not lag behind real time.

### 3) The Medical Alert UI (Frontend)

Build a React dashboard in your **new** frontend project with the following
logic:

- **Filtering:** Only show the latest vitals for each `sensor_id`.
- **Critical Thresholds:**
  - Heart Rate: > 120 bpm
  - SpO2: < 92%
- **Performance:** Use some form of throttling so the UI remains responsive during spikes.

## Definition of Done (DoD)

- [ ] **Data Minimization:** `payload.noise` is discarded immediately.
- [ ] **Concurrency:** `pond` is used for JSON unmarshaling.
- [ ] **Verification:** Sequence numbers in logs remain sequential during spikes.
- [ ] **Alerting:** Red visual cues trigger correctly on threshold breaches.
- [ ] **Contract Compliance:** Extraction struct includes only `sequence` and `vitals`.
- [ ] **UI Performance:** UI remains responsive under spike load.


