package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type TUIScreen int

const (
	ScreenDashboard TUIScreen = 1
	ScreenQueues    TUIScreen = 2
	ScreenStreams   TUIScreen = 3
	ScreenConsumers TUIScreen = 4
	ScreenDLQ       TUIScreen = 5
)

const (
	colorReset   = "\033[0m"
	colorBold    = "\033[1m"
	colorDim     = "\033[2m"
	colorRed     = "\033[31m"
	colorGreen   = "\033[32m"
	colorYellow  = "\033[33m"
	colorBlue    = "\033[34m"
	colorMagenta = "\033[35m"
	colorCyan    = "\033[36m"
	colorWhite   = "\033[37m"
	colorGray    = "\033[90m"
	bgBlue       = "\033[44m"
	bgDarkGray   = "\033[100m"
	clearScreen  = "\033[2J\033[H"
)

type TUIApp struct {
	addr         string
	token        string
	client       *http.Client
	currentScreen TUIScreen
	mu           sync.Mutex
	statusMsg    string
	selectedQueue string
	selectedStream string
}

func NewTUIApp(addr, token string) *TUIApp {
	return &TUIApp{
		addr:          strings.TrimRight(addr, "/"),
		token:         token,
		client:        &http.Client{Timeout: 3 * time.Second},
		currentScreen: ScreenDashboard,
		statusMsg:     "Ready. Press 1-5 to switch tabs, 'r' to refresh, 'q' to quit.",
	}
}

func (app *TUIApp) fetchJSON(path string, target any) error {
	req, err := http.NewRequest(http.MethodGet, app.addr+path, nil)
	if err != nil {
		return err
	}
	if app.token != "" {
		req.Header.Set("Authorization", "Bearer "+app.token)
	}
	resp, err := app.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}
	return json.NewDecoder(resp.Body).Decode(target)
}

func (app *TUIApp) postJSON(path string, body any, target any) error {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = strings.NewReader(string(b))
	}
	req, err := http.NewRequest(http.MethodPost, app.addr+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if app.token != "" {
		req.Header.Set("Authorization", "Bearer "+app.token)
	}
	resp, err := app.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}
	if target != nil {
		return json.NewDecoder(resp.Body).Decode(target)
	}
	return nil
}

func (app *TUIApp) Run(ctx context.Context) error {
	// Hide cursor on enter, restore on exit
	fmt.Print("\033[?25l")
	defer fmt.Print("\033[?25h\033[0m\n")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	keyCh := make(chan string, 16)
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			r, _, err := reader.ReadRune()
			if err != nil {
				return
			}
			keyCh <- string(r)
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Initial render
	app.render()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sigCh:
			return nil
		case key := <-keyCh:
			switch key {
			case "q", "Q":
				return nil
			case "1":
				app.mu.Lock()
				app.currentScreen = ScreenDashboard
				app.statusMsg = "Dashboard view"
				app.mu.Unlock()
				app.render()
			case "2":
				app.mu.Lock()
				app.currentScreen = ScreenQueues
				app.statusMsg = "Queue Inspector [p: pause, u: resume, x: purge, m: pull]"
				app.mu.Unlock()
				app.render()
			case "3":
				app.mu.Lock()
				app.currentScreen = ScreenStreams
				app.statusMsg = "Stream Live Tail"
				app.mu.Unlock()
				app.render()
			case "4":
				app.mu.Lock()
				app.currentScreen = ScreenConsumers
				app.statusMsg = "Consumer Groups & Partition Assignments"
				app.mu.Unlock()
				app.render()
			case "5":
				app.mu.Lock()
				app.currentScreen = ScreenDLQ
				app.statusMsg = "DLQ Dashboard [R: replay all, A: archive]"
				app.mu.Unlock()
				app.render()
			case "r", "R":
				app.mu.Lock()
				app.statusMsg = "Refreshed at " + time.Now().Format("15:04:05")
				app.mu.Unlock()
				app.render()
			case "p", "P":
				if app.currentScreen == ScreenQueues && app.selectedQueue != "" {
					err := app.postJSON("/queues/"+app.selectedQueue+"/pause", nil, nil)
					app.mu.Lock()
					if err != nil {
						app.statusMsg = "Pause failed: " + err.Error()
					} else {
						app.statusMsg = "Queue '" + app.selectedQueue + "' paused."
					}
					app.mu.Unlock()
					app.render()
				}
			case "u", "U":
				if app.currentScreen == ScreenQueues && app.selectedQueue != "" {
					err := app.postJSON("/queues/"+app.selectedQueue+"/resume", nil, nil)
					app.mu.Lock()
					if err != nil {
						app.statusMsg = "Resume failed: " + err.Error()
					} else {
						app.statusMsg = "Queue '" + app.selectedQueue + "' resumed."
					}
					app.mu.Unlock()
					app.render()
				}
			}
		case <-ticker.C:
			app.render()
		}
	}
}

func (app *TUIApp) renderHeader() string {
	var sb strings.Builder
	sb.WriteString(colorBold + colorCyan + " ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n" + colorReset)
	sb.WriteString(colorBold + colorCyan + " ┃" + colorWhite + "   ⚡ BROKER HIGH-PERFORMANCE CLUSTER CONSOLE (brokerctl TUI)       " + colorCyan + "┃\n" + colorReset)
	sb.WriteString(colorBold + colorCyan + " ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n" + colorReset)

	tabs := []struct {
		screen TUIScreen
		name   string
	}{
		{ScreenDashboard, "1: Dashboard"},
		{ScreenQueues, "2: Queues"},
		{ScreenStreams, "3: Streams"},
		{ScreenConsumers, "4: Consumers"},
		{ScreenDLQ, "5: DLQ"},
	}

	sb.WriteString(" ")
	for _, t := range tabs {
		if t.screen == app.currentScreen {
			sb.WriteString(bgBlue + colorWhite + colorBold + " [" + t.name + "] " + colorReset + " ")
		} else {
			sb.WriteString(colorDim + " " + t.name + " " + colorReset + " ")
		}
	}
	sb.WriteString("\n" + colorGray + " ────────────────────────────────────────────────────────────────────────────\n" + colorReset)
	return sb.String()
}

func (app *TUIApp) renderFooter() string {
	return colorGray + " ────────────────────────────────────────────────────────────────────────────\n" +
		colorBold + colorYellow + " Status: " + colorReset + app.statusMsg + "\n" +
		colorDim + " Keys: [1-5]: Switch Tabs | [r]: Refresh | [q]: Quit | Target: " + app.addr + colorReset + "\n"
}

func (app *TUIApp) render() {
	app.mu.Lock()
	defer app.mu.Unlock()

	var content string
	switch app.currentScreen {
	case ScreenDashboard:
		content = app.renderDashboard()
	case ScreenQueues:
		content = app.renderQueues()
	case ScreenStreams:
		content = app.renderStreams()
	case ScreenConsumers:
		content = app.renderConsumers()
	case ScreenDLQ:
		content = app.renderDLQ()
	}

	fmt.Print(clearScreen + app.renderHeader() + content + "\n" + app.renderFooter())
}

func (app *TUIApp) renderDashboard() string {
	var stats struct {
		Queues     map[string]int `json:"queues"`
		Topics     map[string]int `json:"topics"`
		Streams    map[string]int `json:"streams"`
		DLQ        map[string]int `json:"dlq"`
		Messages   int            `json:"messages"`
		Leased     int            `json:"leased"`
		DeadLetter int            `json:"dead_letter"`
	}
	if err := app.fetchJSON("/stats", &stats); err != nil {
		return colorRed + " Error fetching stats: " + err.Error() + colorReset + "\n"
	}

	var sb strings.Builder
	sb.WriteString(colorBold + " 📊 Broker Health & Metrics\n" + colorReset)
	sb.WriteString(fmt.Sprintf("   • Stored Messages : %s%d%s\n", colorGreen+colorBold, stats.Messages, colorReset))
	sb.WriteString(fmt.Sprintf("   • Active Leases   : %s%d%s\n", colorYellow+colorBold, stats.Leased, colorReset))
	sb.WriteString(fmt.Sprintf("   • Dead Letters    : %s%d%s\n", colorRed+colorBold, stats.DeadLetter, colorReset))
	sb.WriteString(fmt.Sprintf("   • Total Queues    : %d | Topics: %d | Streams: %d\n\n", len(stats.Queues), len(stats.Topics), len(stats.Streams)))

	sb.WriteString(colorBold + " 📥 Active Queues (Top 5)\n" + colorReset)
	type qEntry struct {
		name  string
		count int
	}
	var qList []qEntry
	for k, v := range stats.Queues {
		qList = append(qList, qEntry{k, v})
	}
	sort.Slice(qList, func(i, j int) bool { return qList[i].count > qList[j].count })
	for i := 0; i < len(qList) && i < 5; i++ {
		sb.WriteString(fmt.Sprintf("   %-25s  Depth: %s%d%s\n", qList[i].name, colorCyan, qList[i].count, colorReset))
	}
	if len(qList) == 0 {
		sb.WriteString(colorDim + "   (No active queues)\n" + colorReset)
	}

	sb.WriteString("\n" + colorBold + " 💀 Dead-Letter Queues\n" + colorReset)
	var dlqList []qEntry
	for k, v := range stats.DLQ {
		dlqList = append(dlqList, qEntry{k, v})
	}
	sort.Slice(dlqList, func(i, j int) bool { return dlqList[i].count > dlqList[j].count })
	for i := 0; i < len(dlqList) && i < 5; i++ {
		sb.WriteString(fmt.Sprintf("   %-25s  Messages: %s%d%s\n", dlqList[i].name, colorRed, dlqList[i].count, colorReset))
	}
	if len(dlqList) == 0 {
		sb.WriteString(colorDim + "   (No messages in DLQ)\n" + colorReset)
	}

	return sb.String()
}

func (app *TUIApp) renderQueues() string {
	var queues map[string]int
	if err := app.fetchJSON("/queues", &queues); err != nil {
		return colorRed + " Error fetching queues: " + err.Error() + colorReset + "\n"
	}

	var sb strings.Builder
	sb.WriteString(colorBold + " 📦 Queue Inspector\n" + colorReset)
	sb.WriteString(fmt.Sprintf(" %-24s  %-10s  %-12s\n", "QUEUE NAME", "DEPTH", "ACTIONS"))
	sb.WriteString(" ────────────────────────────────────────────────────────────────────────────\n")

	var names []string
	for k := range queues {
		names = append(names, k)
	}
	sort.Strings(names)

	if app.selectedQueue == "" && len(names) > 0 {
		app.selectedQueue = names[0]
	}

	for _, name := range names {
		prefix := "  "
		if name == app.selectedQueue {
			prefix = colorGreen + "▶ " + colorReset
		}
		sb.WriteString(fmt.Sprintf("%s%-22s  %-10d  %s[p]ause [u]nause [x]purge%s\n", prefix, name, queues[name], colorDim, colorReset))
	}
	if len(names) == 0 {
		sb.WriteString(colorDim + "  (No queues declared)\n" + colorReset)
	}
	return sb.String()
}

func (app *TUIApp) renderStreams() string {
	var streams map[string]int
	if err := app.fetchJSON("/streams", &streams); err != nil {
		return colorRed + " Error fetching streams: " + err.Error() + colorReset + "\n"
	}

	var sb strings.Builder
	sb.WriteString(colorBold + " 🌊 Streams & Partition Log Offsets\n" + colorReset)
	sb.WriteString(fmt.Sprintf(" %-30s  %-15s\n", "STREAM NAME", "CURRENT OFFSET"))
	sb.WriteString(" ────────────────────────────────────────────────────────────────────────────\n")

	for name, count := range streams {
		sb.WriteString(fmt.Sprintf("  %-30s  %s%d%s\n", name, colorCyan+colorBold, count, colorReset))
	}
	if len(streams) == 0 {
		sb.WriteString(colorDim + "  (No active streams)\n" + colorReset)
	}
	return sb.String()
}

func (app *TUIApp) renderConsumers() string {
	var consumers []struct {
		ConsumerID   string   `json:"consumer_id"`
		Group        string   `json:"group"`
		Queues       []string `json:"queues"`
		Healthy      bool     `json:"healthy"`
		ActiveTasks  int      `json:"active_tasks"`
		MaxInFlight  int      `json:"max_in_flight"`
		LastActiveAt string   `json:"last_active_at"`
	}
	if err := app.fetchJSON("/v1/consumers", &consumers); err != nil {
		return colorRed + " Error fetching consumers: " + err.Error() + colorReset + "\n"
	}

	var sb strings.Builder
	sb.WriteString(colorBold + " 👥 Registered Consumers & Workers\n" + colorReset)
	sb.WriteString(fmt.Sprintf(" %-20s  %-15s  %-10s  %-10s\n", "CONSUMER ID", "GROUP", "STATUS", "IN-FLIGHT"))
	sb.WriteString(" ────────────────────────────────────────────────────────────────────────────\n")

	for _, c := range consumers {
		status := colorGreen + "HEALTHY" + colorReset
		if !c.Healthy {
			status = colorRed + "UNHEALTHY" + colorReset
		}
		sb.WriteString(fmt.Sprintf("  %-19s  %-15s  %-10s  %d/%d\n", c.ConsumerID, c.Group, status, c.ActiveTasks, c.MaxInFlight))
	}
	if len(consumers) == 0 {
		sb.WriteString(colorDim + "  (No registered consumers)\n" + colorReset)
	}
	return sb.String()
}

func (app *TUIApp) renderDLQ() string {
	var dash struct {
		TotalDepth int `json:"total_depth"`
		QueueCount int `json:"queue_count"`
		Queues     []struct {
			Name      string `json:"name"`
			Depth     int    `json:"depth"`
			LastError string `json:"last_error,omitempty"`
			TopErrors []struct {
				Error string `json:"error"`
				Count int    `json:"count"`
			} `json:"top_errors"`
		} `json:"queues"`
	}

	if err := app.fetchJSON("/dlq/dashboard", &dash); err != nil {
		return colorRed + " Error fetching DLQ dashboard: " + err.Error() + colorReset + "\n"
	}

	var sb strings.Builder
	sb.WriteString(colorBold + " 💀 Dead-Letter Queue Dashboard\n" + colorReset)
	sb.WriteString(fmt.Sprintf(" Total DLQ Messages: %s%d%s across %d queues\n\n", colorRed+colorBold, dash.TotalDepth, colorReset, dash.QueueCount))

	for _, q := range dash.Queues {
		sb.WriteString(fmt.Sprintf(" %s%s%s (Depth: %s%d%s)\n", colorBold+colorRed, q.Name, colorReset, colorYellow, q.Depth, colorReset))
		if q.LastError != "" {
			sb.WriteString(fmt.Sprintf("   • Last Error: %s%s%s\n", colorDim, q.LastError, colorReset))
		}
		if len(q.TopErrors) > 0 {
			sb.WriteString("   • Top Error Patterns:\n")
			for _, te := range q.TopErrors {
				sb.WriteString(fmt.Sprintf("     - [%dx] %s\n", te.Count, te.Error))
			}
		}
		sb.WriteString("\n")
	}

	if len(dash.Queues) == 0 {
		sb.WriteString(colorDim + "  (No dead-letter queues found)\n" + colorReset)
	}
	return sb.String()
}
