package main

import (
	"encoding/json"
	"fmt"
	"html"
	"os"
	"strconv"
	"strings"
	"time"
)

// nodeGroup is one block of /nodes. A cascade lists its entry first and its
// exit second, so the two halves of one chain are read together.
type nodeGroup struct {
	Title   string     `json:"title"`
	Cascade bool       `json:"cascade,omitempty"`
	Nodes   []nodeRole `json:"nodes"`
}

type nodeRole struct {
	Name  string `json:"name"`
	Label string `json:"label,omitempty"`
}

var defaultNodeLayout = []nodeGroup{
	{Title: "🇱🇻 Латвия — обходы", Nodes: []nodeRole{{Name: "lv"}}},
	// Relays forward clients to Latvia in the kernel: their connections are
	// real people, they just enter through another country's address.
	{Title: "🔀 Входы в Латвию", Nodes: []nodeRole{{Name: "relay-lt", Label: "🇱🇹 Литва → lv"}}},
	{Title: "🔗 Каскад 🇷🇺 → 🇳🇱", Cascade: true, Nodes: []nodeRole{{Name: "ru-entry", Label: "вход 🇷🇺"}, {Name: "nl-exit", Label: "выход 🇳🇱"}}},
	{Title: "🔓 Белые списки", Nodes: []nodeRole{{Name: "bs"}}},
	{Title: "🤖 Бот и подписки", Nodes: []nodeRole{{Name: "bot"}}},
}

// nodeLayout lets new nodes (relays, extra exits) be placed without a release:
// NEURA_HELLO_LAYOUT='[{"title":"…","nodes":[{"name":"de-relay"}]}]'.
func nodeLayout() []nodeGroup {
	if raw := strings.TrimSpace(os.Getenv("NEURA_HELLO_LAYOUT")); raw != "" {
		var groups []nodeGroup
		if json.Unmarshal([]byte(raw), &groups) == nil && len(groups) > 0 {
			return groups
		}
	}
	return defaultNodeLayout
}

func groupNumber(n int) string {
	s := strconv.Itoa(n)
	if n < 10000 {
		return s
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteString(" ")
		}
		b.WriteRune(r)
	}
	return b.String()
}

func plural(n int, one, few, many string) string {
	mod10, mod100 := n%10, n%100
	switch {
	case mod10 == 1 && mod100 != 11:
		return one
	case mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14):
		return few
	default:
		return many
	}
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

func warnIf(cond bool) string {
	if cond {
		return " ⚠️"
	}
	return ""
}

func renderNodes(nodes map[string]nodeState, online map[string]int, onlineAt, now time.Time) string {
	var b strings.Builder
	fmt.Fprintf(&b, "🛰 <b>neura-hello</b> · %s\n", now.In(neuraMSK).Format("02.01 15:04 МСК"))
	if len(nodes) == 0 {
		b.WriteString("\nпока ни одна нода не прислала отчёт")
		return b.String()
	}
	alive := 0
	for _, st := range nodes {
		if now.Sub(st.Last.Time) <= alertSilentAfter {
			alive++
		}
	}
	totalOnline, onlineFresh := 0, len(online) > 0 && now.Sub(onlineAt) < 10*time.Minute
	for _, n := range online {
		totalOnline += n
	}
	summary := fmt.Sprintf("на связи %d из %d нод", alive, len(nodes))
	if alive < len(nodes) {
		summary = "🔴 " + summary
	}
	if onlineFresh {
		summary = fmt.Sprintf("👥 онлайн %s · %s", groupNumber(totalOnline), summary)
	}
	b.WriteString(summary + "\n")

	shown := map[string]bool{}
	for _, g := range nodeLayout() {
		present := []nodeRole{}
		for _, role := range g.Nodes {
			if _, ok := nodes[role.Name]; ok {
				present = append(present, role)
			}
		}
		if len(present) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n<b>%s</b>\n", html.EscapeString(g.Title))
		for i, role := range present {
			shown[role.Name] = true
			st := nodes[role.Name]
			if g.Cascade && i > 0 {
				b.WriteString("   ↓\n")
			}
			renderNode(&b, role, st, online, onlineFresh, now, g.Cascade && i > 0)
		}
	}
	rest := []nodeRole{}
	for _, name := range sortedNodeNames(nodes) {
		if !shown[name] {
			rest = append(rest, nodeRole{Name: name})
		}
	}
	if len(rest) > 0 {
		b.WriteString("\n<b>Прочие</b>\n")
		for _, role := range rest {
			renderNode(&b, role, nodes[role.Name], online, onlineFresh, now, false)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func renderNode(b *strings.Builder, role nodeRole, st nodeState, online map[string]int, onlineFresh bool, now time.Time, cascadeExit bool) {
	r := st.Last
	age := now.Sub(r.Time)
	mark := "🟢"
	if age > alertSilentAfter {
		mark = "🔴"
	}
	head := role.Name
	if role.Label != "" {
		head = role.Label + " · " + role.Name
	}
	when := ""
	if age > time.Minute {
		when = " · " + agoText(age)
	}
	fmt.Fprintf(b, "%s %s%s\n", mark, html.EscapeString(head), when)

	conns := r.totalConns()
	line := ""
	if n, ok := online[role.Name]; ok && onlineFresh {
		line = fmt.Sprintf("онлайн %s · ", groupNumber(n))
	}
	if cascadeExit {
		// The exit only sees the entry: its connections are tunnels, not people.
		line += fmt.Sprintf("%s %s от входа", groupNumber(conns), plural(conns, "туннель", "туннеля", "туннелей"))
	} else {
		line += fmt.Sprintf("%s %s · %s %s", groupNumber(conns), plural(conns, "соединение", "соединения", "соединений"),
			groupNumber(r.Clients), plural(r.Clients, "клиент", "клиента", "клиентов"))
	}
	fmt.Fprintf(b, "   %s\n", line)

	if len(r.Conns) > 1 {
		parts := []string{}
		for _, port := range sortedKeys(r.Conns) {
			if r.Conns[port] > 0 {
				parts = append(parts, fmt.Sprintf("%s · %s", port, groupNumber(r.Conns[port])))
			}
		}
		if len(parts) > 1 {
			fmt.Fprintf(b, "   🔌 %s\n", strings.Join(parts, " │ "))
		}
	}

	cpus := max(r.CPUs, 1)
	fmt.Fprintf(b, "   ⚙️ %.1f/%d%s · 💾 %.0f%%%s · 📶 ↓%.0f ↑%.0f Мбит/с\n",
		r.Load1, cpus, warnIf(r.Load1 >= alertLoadPerCPU*float64(cpus)), r.DiskPct, warnIf(r.DiskPct >= alertDiskPct), r.RxMbps, r.TxMbps)

	peak := 0
	for _, p := range st.History {
		peak = max(peak, p.Conns)
	}
	if peak > conns {
		fmt.Fprintf(b, "   📈 пик за сутки %s\n", groupNumber(peak))
	}

	if r.HelloAt != "" && len(r.Hello) > 0 {
		stalls, empties := []string{}, []string{}
		for _, port := range sortedKeys(r.Hello) {
			h := r.Hello[port]
			if h.Flows == 0 {
				continue
			}
			stall := h.Stalled * 100 / h.Flows
			stalls = append(stalls, fmt.Sprintf("%s %d%%%s", port, stall, warnIf(h.Flows >= alertStallFlows && stall >= alertStallPct)))
			if empty := h.NoData * 100 / h.Flows; empty >= 20 {
				empties = append(empties, fmt.Sprintf("%s %d%%", port, empty))
			}
		}
		if len(stalls) > 0 {
			fmt.Fprintf(b, "   🧊 зависают (%s): %s\n", helloTimeText(r.HelloAt), strings.Join(stalls, " · "))
		}
		if len(empties) > 0 {
			fmt.Fprintf(b, "   ⚪ пустые соединения: %s — сканы или блок\n", strings.Join(empties, " · "))
		}
	}
	for _, sc := range r.Scans {
		fmt.Fprintf(b, "   🕵️ скан: AS%s %s — %d с %d IP\n", html.EscapeString(sc.ASN), html.EscapeString(sc.Name), sc.Flows, sc.IPs)
	}
}
