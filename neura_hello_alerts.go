package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

var neuraMSK = time.FixedZone("МСК", 3*3600)

const (
	alertSilentAfter  = 5 * time.Minute
	alertSilentWindow = 48 * time.Hour
	alertRepeat       = 6 * time.Hour
	alertDiskPct      = 85
	alertDiskClearPct = 80
	alertLoadPerCPU   = 1.5
	alertLoadPoints   = 10
	alertStallPct     = 30
	alertStallFlows   = 150
	alertDropFactor   = 0.4
	alertDropClear    = 0.7
	alertDropBaseline = 300
	alertScanRepeat   = 24 * time.Hour
)

type activeAlert struct {
	Sent     time.Time
	Baseline float64
}

type neuraAlerter struct {
	mu     sync.Mutex
	active map[string]*activeAlert
	once   map[string]time.Time
	send   func(string)
	now    func() time.Time
}

func newNeuraAlerter(send func(string), now func() time.Time) *neuraAlerter {
	return &neuraAlerter{active: map[string]*activeAlert{}, once: map[string]time.Time{}, send: send, now: now}
}

// fire raises a condition alert, repeating it while it lasts.
func (a *neuraAlerter) fire(key, text string, baseline float64) {
	now := a.now()
	if st, ok := a.active[key]; ok {
		if now.Sub(st.Sent) < alertRepeat {
			return
		}
		st.Sent = now
		a.send(text + " (всё ещё)")
		return
	}
	a.active[key] = &activeAlert{Sent: now, Baseline: baseline}
	a.send(text)
}

func (a *neuraAlerter) clear(key, text string) {
	if _, ok := a.active[key]; ok {
		delete(a.active, key)
		if text != "" {
			a.send(text)
		}
	}
}

// event raises a one-off alert at most once per window.
func (a *neuraAlerter) event(key, text string, window time.Duration) {
	now := a.now()
	if last, ok := a.once[key]; ok && now.Sub(last) < window {
		return
	}
	a.once[key] = now
	a.send(text)
}

func meanConns(points []nodePoint, from, to int64) (float64, int) {
	sum, n := 0, 0
	for _, p := range points {
		if p.T >= from && p.T < to {
			sum += p.Conns
			n++
		}
	}
	if n == 0 {
		return 0, 0
	}
	return float64(sum) / float64(n), n
}

func (a *neuraAlerter) evaluate(nodes map[string]nodeState) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	for _, name := range sortedNodeNames(nodes) {
		st := nodes[name]
		last := st.Last
		age := now.Sub(last.Time)

		key := "silent:" + name
		if age > alertSilentAfter && age < alertSilentWindow {
			a.fire(key, fmt.Sprintf("🔴 нода %s молчит %d мин", name, int(age.Minutes())), 0)
			continue
		}
		a.clear(key, fmt.Sprintf("🟢 нода %s снова на связи", name))
		if age >= alertSilentWindow {
			continue
		}

		key = "disk:" + name
		if last.DiskPct >= alertDiskPct {
			a.fire(key, fmt.Sprintf("💾 диск на %s заполнен на %.0f%%", name, last.DiskPct), 0)
		} else if last.DiskPct < alertDiskClearPct {
			a.clear(key, fmt.Sprintf("💾 диск на %s в норме: %.0f%%", name, last.DiskPct))
		}

		key = "load:" + name
		cpus := float64(max(last.CPUs, 1))
		if h := st.History; len(h) >= alertLoadPoints {
			high := true
			for _, p := range h[len(h)-alertLoadPoints:] {
				if p.Load < alertLoadPerCPU*cpus {
					high = false
					break
				}
			}
			if high {
				a.fire(key, fmt.Sprintf("🔥 нагрузка на %s %.1f при %d ядрах уже %d мин", name, last.Load1, last.CPUs, alertLoadPoints), 0)
			} else if last.Load1 < cpus {
				a.clear(key, fmt.Sprintf("✅ нагрузка на %s спала: %.1f", name, last.Load1))
			}
		}

		if last.HelloAt != "" {
			for _, port := range sortedKeys(last.Hello) {
				h := last.Hello[port]
				if h.Flows >= alertStallFlows && h.Stalled*100 >= alertStallPct*h.Flows {
					a.event("stall:"+name+":"+port+":"+last.HelloAt,
						fmt.Sprintf("🧊 %s:%s — %d%% ClientHello зависают (%d соединений за минуту замера)", name, port, h.Stalled*100/h.Flows, h.Flows), alertScanRepeat)
				}
			}
		}
		for _, sc := range last.Scans {
			a.event("scan:"+name+":"+sc.ASN,
				fmt.Sprintf("🕵️ %s сканируют: AS%s %s — %d пустых соединений с %d IP за минуту", name, sc.ASN, sc.Name, sc.Flows, sc.IPs), alertScanRepeat)
		}

		// A sudden drop against the previous hour is what a block looks like.
		key = "drop:" + name
		nowT := last.Time.Unix()
		base, baseN := meanConns(st.History, nowT-3600, nowT-900)
		cur, curN := meanConns(st.History, nowT-300, nowT+1)
		if active, ok := a.active[key]; ok {
			if curN > 0 && cur >= alertDropClear*active.Baseline {
				a.clear(key, fmt.Sprintf("📈 соединения на %s восстановились: %.0f", name, cur))
			}
		} else if baseN >= 20 && curN >= 3 && base >= alertDropBaseline && cur < alertDropFactor*base {
			a.fire(key, fmt.Sprintf("📉 соединения на %s упали: было ~%.0f, сейчас ~%.0f — похоже на блокировку", name, base, cur), base)
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, errA := strconv.Atoi(keys[i])
		b, errB := strconv.Atoi(keys[j])
		if errA == nil && errB == nil {
			return a < b
		}
		return keys[i] < keys[j]
	})
	return keys
}

func renderNodes(nodes map[string]nodeState, online map[string]int, onlineAt, now time.Time) string {
	var b strings.Builder
	fmt.Fprintf(&b, "🛰 neura-hello · %s\n", now.In(neuraMSK).Format("02.01 15:04 МСК"))
	if len(nodes) == 0 {
		b.WriteString("\nпока ни одна нода не прислала отчёт")
		return b.String()
	}
	totalOnline := 0
	for _, n := range online {
		totalOnline += n
	}
	if len(online) > 0 && now.Sub(onlineAt) < 10*time.Minute {
		fmt.Fprintf(&b, "онлайн по панелям: %d\n", totalOnline)
	}
	for _, name := range sortedNodeNames(nodes) {
		st := nodes[name]
		r := st.Last
		age := now.Sub(r.Time)
		mark := "🟢"
		if age > alertSilentAfter {
			mark = "🔴"
		}
		fmt.Fprintf(&b, "\n%s %s · %s\n", mark, name, agoText(age))
		line := fmt.Sprintf("соединений %d · клиентов %d", r.totalConns(), r.Clients)
		if n, ok := online[name]; ok {
			line = fmt.Sprintf("онлайн %d · ", n) + line
		}
		b.WriteString(line + "\n")
		if len(r.Conns) > 1 {
			parts := []string{}
			for _, port := range sortedKeys(r.Conns) {
				if r.Conns[port] > 0 {
					parts = append(parts, fmt.Sprintf("%s:%d", port, r.Conns[port]))
				}
			}
			if len(parts) > 0 {
				b.WriteString("  порты " + strings.Join(parts, " ") + "\n")
			}
		}
		fmt.Fprintf(&b, "  нагрузка %.1f/%d · память %.0f%% · диск %.0f%% · ↓%.0f ↑%.0f Мбит/с\n", r.Load1, r.CPUs, r.MemPct, r.DiskPct, r.RxMbps, r.TxMbps)
		peak := 0
		for _, p := range st.History {
			peak = max(peak, p.Conns)
		}
		if peak > 0 {
			fmt.Fprintf(&b, "  пик за сутки: %d соединений\n", peak)
		}
		if r.HelloAt != "" && len(r.Hello) > 0 {
			parts := []string{}
			for _, port := range sortedKeys(r.Hello) {
				h := r.Hello[port]
				if h.Flows == 0 {
					continue
				}
				parts = append(parts, fmt.Sprintf("%s: завис %d%% фраг %d%% пуст %d%%", port, h.Stalled*100/h.Flows, h.Frag*100/h.Flows, h.NoData*100/h.Flows))
			}
			if len(parts) > 0 {
				b.WriteString("  hello " + helloTimeText(r.HelloAt) + ": " + strings.Join(parts, " · ") + "\n")
			}
		}
		for _, sc := range r.Scans {
			fmt.Fprintf(&b, "  🕵️ скан AS%s %s: %d пустых с %d IP\n", sc.ASN, sc.Name, sc.Flows, sc.IPs)
		}
	}
	return b.String()
}

func agoText(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "только что"
	case d < time.Hour:
		return fmt.Sprintf("%d мин назад", int(d.Minutes()))
	default:
		return fmt.Sprintf("%d ч назад", int(d.Hours()))
	}
}

func helloTimeText(at string) string {
	t, err := time.Parse(time.RFC3339, at)
	if err != nil {
		return at
	}
	return t.In(neuraMSK).Format("15:04")
}

func handleNodesCommand(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) bool {
	if msg == nil || !msg.IsCommand() || msg.Command() != "nodes" {
		return false
	}
	if msg.From == nil || !isAdmin(msg.From.ID) {
		return false
	}
	nodes, online, onlineAt := neuraNodes.snapshot()
	_, _ = bot.Send(tgbotapi.NewMessage(msg.Chat.ID, renderNodes(nodes, online, onlineAt, time.Now())))
	return true
}

// neuraPanelNodes maps a panel's server address to a node name, from
// NEURA_HELLO_NODE_ADDRS="lv=213.21.208.254,bs=95.181.226.64".
func neuraPanelNodes() map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(os.Getenv("NEURA_HELLO_NODE_ADDRS"), ",") {
		name, addr, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if ok && neuraHelloNodeName.MatchString(name) && addr != "" {
			out[strings.TrimSpace(addr)] = name
		}
	}
	return out
}

func pollPanelOnline(configs []*xraySettings) map[string]int {
	byAddr := neuraPanelNodes()
	online := map[string]int{}
	for _, cfg := range configs {
		if cfg == nil || cfg.client == nil {
			continue
		}
		name, ok := byAddr[cfg.serverAddress]
		if !ok {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		emails, err := cfg.client.OnlineClients(ctx)
		cancel()
		if err != nil {
			log.Printf("[neura-hello] online count %s: %v", name, err)
			continue
		}
		online[name] += len(emails)
	}
	return online
}

func startNeuraHello(bot *tgbotapi.BotAPI, mainCfg *xraySettings) {
	neuraNodes.load()
	alerter := newNeuraAlerter(func(text string) {
		if _, err := bot.Send(tgbotapi.NewMessage(mainAdminID, text)); err != nil {
			log.Printf("[neura-hello] alert send: %v", err)
		}
	}, time.Now)
	go func() {
		// Let agents report after a restart before judging silence.
		time.Sleep(2 * time.Minute)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		saveEvery := 0
		for {
			configs := append([]*xraySettings{mainCfg}, mergedProviderConfigs()...)
			neuraNodes.setOnline(pollPanelOnline(configs), time.Now())
			nodes, _, _ := neuraNodes.snapshot()
			alerter.evaluate(nodes)
			if saveEvery++; saveEvery >= 5 {
				saveEvery = 0
				if err := neuraNodes.save(); err != nil {
					log.Printf("[neura-hello] save: %v", err)
				}
			}
			<-ticker.C
		}
	}()
}
