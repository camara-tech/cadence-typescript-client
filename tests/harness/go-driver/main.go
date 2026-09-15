package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/compatibility"
	"go.uber.org/cadence/interceptors"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/grpc"

	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
)

type server struct {
	mu         sync.Mutex
	dispatcher *yarpc.Dispatcher

	domainMu sync.Mutex
	clients  map[string]client.Client
	workers  map[string]worker.Worker

	stats    *InterceptorStats
	identity string
}

func main() {
	addr := os.Getenv("SERVICE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:7833"
	}
	listen := os.Getenv("LISTEN_ADDR")
	if listen == "" {
		listen = "127.0.0.1:7877"
	}

	transport := grpc.NewTransport()
	outbound := transport.NewSingleOutbound(addr)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "gwt-driver",
		Outbounds: yarpc.Outbounds{
			"cadence-frontend": {Unary: outbound},
		},
	})
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("dispatcher start: %v", err)
	}

	s := &server{
		dispatcher: dispatcher,
		clients:    map[string]client.Client{},
		workers:    map[string]worker.Worker{},
		stats:      &InterceptorStats{},
		identity:   "gwt-go-driver",
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/ensureDomain", s.handleEnsureDomain)
	mux.HandleFunc("/startWorkflow", s.handleStartWorkflow)
	mux.HandleFunc("/signalWorkflow", s.handleSignalWorkflow)
	mux.HandleFunc("/signalWithStartWorkflow", s.handleSignalWithStartWorkflow)
	mux.HandleFunc("/queryWorkflow", s.handleQueryWorkflow)
	mux.HandleFunc("/cancelWorkflow", s.handleCancelWorkflow)
	mux.HandleFunc("/terminateWorkflow", s.handleTerminateWorkflow)
	mux.HandleFunc("/waitForClose", s.handleWaitForClose)
	mux.HandleFunc("/describeWorkflowExecution", s.handleDescribeWorkflowExecution)
	mux.HandleFunc("/getHistory", s.handleGetHistory)
	mux.HandleFunc("/listOpenWorkflowExecutions", s.handleListOpen)
	mux.HandleFunc("/listClosedWorkflowExecutions", s.handleListClosed)
	mux.HandleFunc("/interceptorStats", s.handleInterceptorStats)

	log.Printf("gwt-driver listening on %s (cadence %s)", listen, addr)
	log.Fatal(http.ListenAndServe(listen, mux))
}

// ---- plumbing ----

func (s *server) serviceClient() workflowserviceclient.Interface {
	cfg := s.dispatcher.ClientConfig("cadence-frontend")
	return compatibility.NewThrift2ProtoAdapter(compatibility.AdapterClients{
		Domain:     apiv1.NewDomainAPIYARPCClient(cfg),
		Workflow:   apiv1.NewWorkflowAPIYARPCClient(cfg),
		Worker:     apiv1.NewWorkerAPIYARPCClient(cfg),
		Visibility: apiv1.NewVisibilityAPIYARPCClient(cfg),
	})
}

func (s *server) clientFor(domain string) client.Client {
	s.domainMu.Lock()
	defer s.domainMu.Unlock()
	if c, ok := s.clients[domain]; ok {
		return c
	}
	c := client.NewClient(s.serviceClient(), domain, &client.Options{Identity: s.identity})
	s.clients[domain] = c
	return c
}

func (s *server) workerFor(domain, taskList string) (worker.Worker, error) {
	s.domainMu.Lock()
	defer s.domainMu.Unlock()
	key := domain + "/" + taskList
	if w, ok := s.workers[key]; ok {
		return w, nil
	}
	w, err := worker.NewV2(s.serviceClient(), domain, taskList, worker.Options{
		Identity:                          s.identity,
		WorkflowInterceptorChainFactories: []interceptors.WorkflowInterceptorFactory{&countingFactory{stats: s.stats}},
	})
	if err != nil {
		return nil, err
	}
	registerBehaviors(w)
	if err := w.Start(); err != nil {
		return nil, err
	}
	s.workers[key] = w
	return w, nil
}

// ---- error mapping ----

func mapError(err error) (int, string) {
	var alreadyStarted *shared.WorkflowExecutionAlreadyStartedError
	if errors.As(err, &alreadyStarted) {
		return 409, "WorkflowExecutionAlreadyStarted"
	}
	var notExists *shared.EntityNotExistsError
	if errors.As(err, &notExists) {
		return 404, "EntityNotExists"
	}
	var domainExists *shared.DomainAlreadyExistsError
	if errors.As(err, &domainExists) {
		return 409, "DomainAlreadyExists"
	}
	var badRequest *shared.BadRequestError
	if errors.As(err, &badRequest) {
		return 400, "BadRequest"
	}
	return 500, "Internal"
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func logReq(name string, v interface{}) {
	if b, err := json.Marshal(v); err == nil {
		log.Printf("%s %s", name, string(b))
	}
}

func readJSON(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

func fail(w http.ResponseWriter, err error) {
	status, code := mapError(err)
	writeJSON(w, status, map[string]string{"code": code, "message": err.Error()})
}

// ---- handlers ----

func (s *server) handleEnsureDomain(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name          string `json:"name"`
		RetentionDays int32  `json:"retentionDays"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	if req.RetentionDays == 0 {
		req.RetentionDays = 1
	}
	dc := client.NewDomainClient(s.serviceClient(), &client.Options{Identity: s.identity})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := dc.Register(ctx, &shared.RegisterDomainRequest{
		Name:                                   &req.Name,
		WorkflowExecutionRetentionPeriodInDays: &req.RetentionDays,
	})
	if err != nil {
		var exists *shared.DomainAlreadyExistsError
		if errors.As(err, &exists) {
			w.WriteHeader(204)
			return
		}
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *server) handleStartWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain                          string                 `json:"domain"`
		WorkflowType                    string                 `json:"workflowType"`
		TaskList                        string                 `json:"taskList"`
		Input                           []json.RawMessage      `json:"input"`
		ExecutionStartToCloseTimeoutMs  int64                  `json:"executionStartToCloseTimeoutMs"`
		DecisionTaskStartToCloseTimeoutMs int64                `json:"decisionTaskStartToCloseTimeoutMs"`
		WorkflowID                      string                 `json:"workflowId"`
		WorkflowIDReusePolicy           string                 `json:"workflowIdReusePolicy"`
		RetryPolicy                     *retryPolicyJSON       `json:"retryPolicy"`
		CronSchedule                    string                 `json:"cronSchedule"`
		Memo                            map[string]interface{} `json:"memo"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	if _, err := s.workerFor(req.Domain, req.TaskList); err != nil {
		fail(w, err)
		return
	}
	c := s.clientFor(req.Domain)
	opts := client.StartWorkflowOptions{
		ID:                              req.WorkflowID,
		TaskList:                        req.TaskList,
		ExecutionStartToCloseTimeout:    time.Duration(req.ExecutionStartToCloseTimeoutMs) * time.Millisecond,
		DecisionTaskStartToCloseTimeout: time.Duration(req.DecisionTaskStartToCloseTimeoutMs) * time.Millisecond,
		WorkflowIDReusePolicy:           reusePolicy(req.WorkflowIDReusePolicy),
		CronSchedule:                    req.CronSchedule,
	}
	if req.DecisionTaskStartToCloseTimeoutMs == 0 {
		opts.DecisionTaskStartToCloseTimeout = 10 * time.Second
	}
	if req.RetryPolicy != nil {
		opts.RetryPolicy = req.RetryPolicy.toInternal()
	}
	if req.Memo != nil {
		opts.Memo = req.Memo
	}
	args, err := decodeArgs(req.Input)
	if err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	run, err := c.StartWorkflow(ctx, opts, req.WorkflowType, args...)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"workflowId": run.ID, "runId": run.RunID})
}

func (s *server) handleSignalWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain     string      `json:"domain"`
		WorkflowID string      `json:"workflowId"`
		RunID      string      `json:"runId"`
		SignalName string      `json:"signalName"`
		Input      interface{} `json:"input"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	c := s.clientFor(req.Domain)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.SignalWorkflow(ctx, req.WorkflowID, req.RunID, req.SignalName, req.Input); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *server) handleSignalWithStartWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain                         string            `json:"domain"`
		WorkflowID                     string            `json:"workflowId"`
		SignalName                     string            `json:"signalName"`
		SignalInput                    interface{}       `json:"signalInput"`
		WorkflowType                   string            `json:"workflowType"`
		TaskList                       string            `json:"taskList"`
		Input                          []json.RawMessage `json:"input"`
		ExecutionStartToCloseTimeoutMs int64             `json:"executionStartToCloseTimeoutMs"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	if _, werr := s.workerFor(req.Domain, req.TaskList); werr != nil {
		fail(w, werr)
		return
	}
	c := s.clientFor(req.Domain)
	args, err := decodeArgs(req.Input)
	if err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := c.SignalWithStartWorkflow(ctx, req.WorkflowID, req.SignalName, req.SignalInput,
		client.StartWorkflowOptions{
			ID:                           req.WorkflowID,
			TaskList:                     req.TaskList,
			ExecutionStartToCloseTimeout: time.Duration(req.ExecutionStartToCloseTimeoutMs) * time.Millisecond,
		}, req.WorkflowType, args...)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"workflowId": resp.ID, "runId": resp.RunID})
}

func (s *server) handleQueryWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain     string `json:"domain"`
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
		QueryType  string `json:"queryType"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	c := s.clientFor(req.Domain)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	value, err := c.QueryWorkflow(ctx, req.WorkflowID, req.RunID, req.QueryType)
	if err != nil {
		fail(w, err)
		return
	}
	var out interface{}
	if err := value.Get(&out); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]interface{}{"value": out})
}

func (s *server) handleCancelWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain     string `json:"domain"`
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	c := s.clientFor(req.Domain)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.CancelWorkflow(ctx, req.WorkflowID, req.RunID); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *server) handleTerminateWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain     string `json:"domain"`
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
		Reason     string `json:"reason"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	c := s.clientFor(req.Domain)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.TerminateWorkflow(ctx, req.WorkflowID, req.RunID, req.Reason, nil); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *server) handleWaitForClose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain     string `json:"domain"`
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
		TimeoutMs  int64  `json:"timeoutMs"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	c := s.clientFor(req.Domain)
	timeout := 60 * time.Second
	if req.TimeoutMs > 0 {
		timeout = time.Duration(req.TimeoutMs) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	run := c.GetWorkflow(ctx, req.WorkflowID, req.RunID)
	var result interface{}
	err := run.Get(ctx, &result)
	if err == nil {
		writeJSON(w, 200, map[string]interface{}{"status": "COMPLETED", "result": result})
		return
	}
	// Distinguish "execution never existed" from a closed-with-failure run.
	var notExists *shared.EntityNotExistsError
	if errors.As(err, &notExists) {
		writeJSON(w, 404, map[string]string{"code": "EntityNotExists", "message": err.Error()})
		return
	}
	out := closeResultFromError(err)
	// The error chain may not reflect the run's authoritative close status
	// (e.g. an activity timeout fails the workflow). Ask the server.
	if resp, derr := c.DescribeWorkflowExecution(ctx, req.WorkflowID, req.RunID); derr == nil &&
		resp.WorkflowExecutionInfo.CloseStatus != nil {
		out["status"] = closeStatusString(resp.WorkflowExecutionInfo.CloseStatus)
	}
	writeJSON(w, 200, out)
}

// closeResultFromError maps a workflow close error into the port's result shape.
func closeResultFromError(err error) map[string]interface{} {
	var canceled *cadence.CanceledError
	if errors.As(err, &canceled) {
		return map[string]interface{}{
			"status":  "CANCELED",
			"failure": map[string]string{"type": "CanceledError", "message": err.Error()},
		}
	}
	var terminated *workflow.TerminatedError
	if errors.As(err, &terminated) {
		return map[string]interface{}{
			"status":  "TERMINATED",
			"failure": map[string]string{"type": "TerminatedError", "message": err.Error()},
		}
	}
	var timeout *workflow.TimeoutError
	if errors.As(err, &timeout) {
		return map[string]interface{}{
			"status":  "TIMED_OUT",
			"failure": map[string]string{"type": "Timeout", "message": err.Error()},
		}
	}
	var custom *cadence.CustomError
	if errors.As(err, &custom) {
		return map[string]interface{}{
			"status": "FAILED",
			"failure": map[string]string{
				"type":    "ApplicationError",
				"message": err.Error(),
				"reason":  custom.Reason(),
			},
		}
	}
	return map[string]interface{}{
		"status":  "FAILED",
		"failure": map[string]string{"type": "Generic", "message": err.Error()},
	}
}

func (s *server) handleDescribeWorkflowExecution(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain     string `json:"domain"`
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	c := s.clientFor(req.Domain)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := c.DescribeWorkflowExecution(ctx, req.WorkflowID, req.RunID)
	if err != nil {
		fail(w, err)
		return
	}
	status := "RUNNING"
	if resp.WorkflowExecutionInfo.CloseStatus != nil {
		status = closeStatusString(resp.WorkflowExecutionInfo.CloseStatus)
	}
	attempts := map[string]int32{}
	for _, pa := range resp.PendingActivities {
		if pa.ActivityID != nil {
			attempts[pa.GetActivityID()] = pa.GetAttempt()
		}
	}
	writeJSON(w, 200, map[string]interface{}{
		"status":          status,
		"historyLength":   resp.WorkflowExecutionInfo.GetHistoryLength(),
		"activityAttempts": attempts,
	})
}

func (s *server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain     string `json:"domain"`
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	c := s.clientFor(req.Domain)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	iter := c.GetWorkflowHistory(ctx, req.WorkflowID, req.RunID, false, shared.HistoryEventFilterTypeAllEvent)
	var events []map[string]interface{}
	for iter.HasNext() {
		e, err := iter.Next()
		if err != nil {
			fail(w, err)
			return
		}
		events = append(events, map[string]interface{}{
			"eventType": e.GetEventType().String(),
			"id":        e.GetEventId(),
		})
	}
	writeJSON(w, 200, events)
}

func (s *server) handleListOpen(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain     string `json:"domain"`
		MaxResults int32  `json:"maxResults"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	c := s.clientFor(req.Domain)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	earliest := time.Now().Add(-24 * time.Hour).UnixNano()
	resp, err := c.ListOpenWorkflow(ctx, &shared.ListOpenWorkflowExecutionsRequest{
		Domain:          &req.Domain,
			StartTimeFilter: &shared.StartTimeFilter{EarliestTime: &earliest, LatestTime: func() *int64 { v := time.Now().Add(time.Hour).UnixNano(); return &v }()},
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, summarize(resp.Executions))
}

func (s *server) handleListClosed(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domain     string `json:"domain"`
		MaxResults int32  `json:"maxResults"`
		WorkflowID string `json:"workflowId"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"code": "BadRequest", "message": err.Error()})
		return
	}
	logReq("request", req)
	c := s.clientFor(req.Domain)
	listReq := &shared.ListClosedWorkflowExecutionsRequest{
		Domain:          &req.Domain,
		MaximumPageSize: &req.MaxResults,
		StartTimeFilter: &shared.StartTimeFilter{
			EarliestTime: func() *int64 { v := time.Now().Add(-24 * time.Hour).UnixNano(); return &v }(),
			LatestTime:   func() *int64 { v := time.Now().Add(time.Hour).UnixNano(); return &v }(),
		},

	}
	if req.WorkflowID != "" {
		listReq.ExecutionFilter = &shared.WorkflowExecutionFilter{WorkflowId: &req.WorkflowID}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := c.ListClosedWorkflow(ctx, listReq)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, summarize(resp.Executions))
}

func (s *server) handleInterceptorStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]int32{
		"workflowStarts": atomic.LoadInt32(&s.stats.WorkflowStarts),
		"activityStarts": atomic.LoadInt32(&s.stats.ActivityStarts),
	})
}

// ---- helpers ----

// ---- helpers ----

// retryPolicyJSON is the JSON shape of a retry policy on the wire.
type retryPolicyJSON struct {
	InitialIntervalMs        int64    `json:"initialIntervalMs"`
	BackoffCoefficient       float64  `json:"backoffCoefficient"`
	MaximumIntervalMs        int64    `json:"maximumIntervalMs"`
	ExpirationIntervalMs     int64    `json:"expirationIntervalMs"`
	MaximumAttempts          int32    `json:"maximumAttempts"`
	NonRetriableErrorReasons []string `json:"nonRetriableErrorReasons"`
}

func (p *retryPolicyJSON) toInternal() *workflow.RetryPolicy {
	return &workflow.RetryPolicy{
		InitialInterval:          time.Duration(p.InitialIntervalMs) * time.Millisecond,
		BackoffCoefficient:       p.BackoffCoefficient,
		MaximumInterval:          time.Duration(p.MaximumIntervalMs) * time.Millisecond,
		ExpirationInterval:       time.Duration(p.ExpirationIntervalMs) * time.Millisecond,
		MaximumAttempts:          p.MaximumAttempts,
		NonRetriableErrorReasons: p.NonRetriableErrorReasons,
	}
}

func reusePolicy(p string) client.WorkflowIDReusePolicy {
	switch p {
	case "AllowDuplicate":
		return client.WorkflowIDReusePolicyAllowDuplicate
	case "RejectDuplicate":
		return client.WorkflowIDReusePolicyRejectDuplicate
	case "TerminateIfRunning":
		return client.WorkflowIDReusePolicyTerminateIfRunning
	default:
		return client.WorkflowIDReusePolicyAllowDuplicateFailedOnly
	}
}

func closeStatusString(cs *shared.WorkflowExecutionCloseStatus) string {
	if cs == nil {
		return "RUNNING"
	}
	switch *cs {
	case shared.WorkflowExecutionCloseStatusCompleted:
		return "COMPLETED"
	case shared.WorkflowExecutionCloseStatusFailed:
		return "FAILED"
	case shared.WorkflowExecutionCloseStatusCanceled:
		return "CANCELED"
	case shared.WorkflowExecutionCloseStatusTerminated:
		return "TERMINATED"
	case shared.WorkflowExecutionCloseStatusTimedOut:
		return "TIMED_OUT"
	case shared.WorkflowExecutionCloseStatusContinuedAsNew:
		return "CONTINUED_AS_NEW"
	}
	return "UNKNOWN"
}

func summarize(infos []*shared.WorkflowExecutionInfo) []map[string]interface{} {
	var out []map[string]interface{}
	for _, info := range infos {
		item := map[string]interface{}{
			"workflowId": info.Execution.GetWorkflowId(),
			"runId":      info.Execution.GetRunId(),
			"type":       info.Type.GetName(),
			"status":     closeStatusString(info.CloseStatus),
			"startTimeMs": info.GetStartTime(),
		}
		if info.CloseTime != nil {
			item["closeTimeMs"] = info.GetCloseTime()
		}
		if info.Memo != nil {
			memo := map[string]interface{}{}
			for k, p := range info.Memo.Fields {
				var v interface{}
				if err := json.Unmarshal(p, &v); err == nil {
					memo[k] = v
				}
			}
			item["memo"] = memo
		}
		out = append(out, item)
	}
	return out
}

func decodeArgs(raw []json.RawMessage) ([]interface{}, error) {
	var out []interface{}
	for _, r := range raw {
		var v interface{}
		if err := json.Unmarshal(r, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
