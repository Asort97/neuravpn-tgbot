package main

// neura-hello: node health collector. Each VPN node runs a small agent that
// pushes a signed summary every minute (connections, load, disk, traffic and
// the ClientHello statistics). The bot keeps the latest report and a day of
// history, answers /nodes and raises alerts. No client IPs ever arrive here.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	neuraHelloMaxBody    = 64 << 10
	neuraHelloMaxSkew    = 5 * time.Minute
	neuraHelloHistoryAge = 24 * time.Hour
	neuraHelloStatePath  = "database/neura_hello.json"
)

var neuraHelloNodeName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)

type helloPort struct {
	Flows   int `json:"flows"`
	Stalled int `json:"stalled"`
	Frag    int `json:"frag"`
	NoData  int `json:"no_data"`
}

type helloScan struct {
	ASN   string `json:"asn"`
	Name  string `json:"name"`
	Flows int    `json:"flows"`
	IPs   int    `json:"ips"`
}

type nodeReport struct {
	Node    string               `json:"node"`
	Time    time.Time            `json:"time"`
	Load1   float64              `json:"load1"`
	CPUs    int                  `json:"cpus"`
	MemPct  float64              `json:"mem_pct"`
	DiskPct float64              `json:"disk_pct"`
	RxMbps  float64              `json:"rx_mbps"`
	TxMbps  float64              `json:"tx_mbps"`
	Conns   map[string]int       `json:"conns"`
	Clients int                  `json:"clients"`
	HelloAt string               `json:"hello_at,omitempty"`
	Hello   map[string]helloPort `json:"hello,omitempty"`
	Scans   []helloScan          `json:"scans,omitempty"`
}

func (r nodeReport) totalConns() int {
	total := 0
	for _, n := range r.Conns {
		total += n
	}
	return total
}

type nodePoint struct {
	T       int64   `json:"t"`
	Conns   int     `json:"c"`
	Clients int     `json:"u"`
	Load    float64 `json:"l"`
}

type nodeState struct {
	Last    nodeReport  `json:"last"`
	History []nodePoint `json:"history"`
}

type nodeStore struct {
	mu       sync.Mutex
	path     string
	nodes    map[string]*nodeState
	online   map[string]int
	onlineAt time.Time
	dirty    bool
}

func newNodeStore(path string) *nodeStore {
	return &nodeStore{path: path, nodes: map[string]*nodeState{}, online: map[string]int{}}
}

var neuraNodes = newNodeStore(neuraHelloStatePath)

func (s *nodeStore) record(r nodeReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.nodes[r.Node]
	if st == nil {
		st = &nodeState{}
		s.nodes[r.Node] = st
	}
	st.Last = r
	st.History = append(st.History, nodePoint{T: r.Time.Unix(), Conns: r.totalConns(), Clients: r.Clients, Load: r.Load1})
	cutoff := r.Time.Add(-neuraHelloHistoryAge).Unix()
	i := 0
	for i < len(st.History) && st.History[i].T < cutoff {
		i++
	}
	st.History = append([]nodePoint(nil), st.History[i:]...)
	s.dirty = true
}

func (s *nodeStore) setOnline(online map[string]int, at time.Time) {
	s.mu.Lock()
	s.online, s.onlineAt = online, at
	s.mu.Unlock()
}

// snapshot returns deep copies so callers never race with incoming reports.
func (s *nodeStore) snapshot() (map[string]nodeState, map[string]int, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nodes := make(map[string]nodeState, len(s.nodes))
	for name, st := range s.nodes {
		nodes[name] = nodeState{Last: st.Last, History: append([]nodePoint(nil), st.History...)}
	}
	online := make(map[string]int, len(s.online))
	for k, v := range s.online {
		online[k] = v
	}
	return nodes, online, s.onlineAt
}

func (s *nodeStore) load() {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return
	}
	var nodes map[string]*nodeState
	if json.Unmarshal(data, &nodes) != nil {
		return
	}
	s.mu.Lock()
	for name, st := range nodes {
		if st != nil && neuraHelloNodeName.MatchString(name) {
			s.nodes[name] = st
		}
	}
	s.mu.Unlock()
}

func (s *nodeStore) save() error {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return nil
	}
	data, err := json.Marshal(s.nodes)
	s.dirty = false
	s.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func neuraHelloSignature(secret []byte, ts string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts))
	mac.Write([]byte("\n"))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func clampMetric(v, max float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0
	}
	return math.Min(v, max)
}

// sanitizeReport bounds every field so a broken or hostile agent cannot bloat
// state or break rendering.
func sanitizeReport(r *nodeReport) bool {
	if !neuraHelloNodeName.MatchString(r.Node) {
		return false
	}
	r.Load1 = clampMetric(r.Load1, 1e4)
	r.MemPct = clampMetric(r.MemPct, 100)
	r.DiskPct = clampMetric(r.DiskPct, 100)
	r.RxMbps = clampMetric(r.RxMbps, 1e6)
	r.TxMbps = clampMetric(r.TxMbps, 1e6)
	if r.CPUs < 1 || r.CPUs > 1024 {
		r.CPUs = 1
	}
	if r.Clients < 0 {
		r.Clients = 0
	}
	conns := map[string]int{}
	for port, n := range r.Conns {
		if _, err := strconv.Atoi(port); err == nil && n >= 0 && len(conns) < 64 {
			conns[port] = n
		}
	}
	r.Conns = conns
	hello := map[string]helloPort{}
	for port, h := range r.Hello {
		if _, err := strconv.Atoi(port); err == nil && h.Flows >= 0 && len(hello) < 64 {
			hello[port] = h
		}
	}
	r.Hello = hello
	if len(r.HelloAt) > 32 {
		r.HelloAt = r.HelloAt[:32]
	}
	if len(r.Scans) > 10 {
		r.Scans = r.Scans[:10]
	}
	for i := range r.Scans {
		if len(r.Scans[i].Name) > 60 {
			r.Scans[i].Name = r.Scans[i].Name[:60]
		}
		if len(r.Scans[i].ASN) > 12 {
			r.Scans[i].ASN = r.Scans[i].ASN[:12]
		}
	}
	return true
}

func handleNeuraHelloReport(secret []byte, store *nodeStore, now func() time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, neuraHelloMaxBody+1))
		if err != nil || len(body) > neuraHelloMaxBody {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		ts := r.Header.Get("X-Neura-Timestamp")
		sec, err := strconv.ParseInt(ts, 10, 64)
		if err != nil || now().Sub(time.Unix(sec, 0)).Abs() > neuraHelloMaxSkew {
			http.Error(w, "stale", http.StatusUnauthorized)
			return
		}
		want := neuraHelloSignature(secret, ts, body)
		if !hmac.Equal([]byte(want), []byte(r.Header.Get("X-Neura-Signature"))) {
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return
		}
		var report nodeReport
		if json.Unmarshal(body, &report) != nil || !sanitizeReport(&report) {
			http.Error(w, "bad report", http.StatusBadRequest)
			return
		}
		report.Time = now().UTC()
		store.record(report)
		w.WriteHeader(http.StatusNoContent)
	}
}

func sortedNodeNames(nodes map[string]nodeState) []string {
	names := make([]string, 0, len(nodes))
	for name := range nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
