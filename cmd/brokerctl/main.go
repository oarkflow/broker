package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const maxBrokerctlResponseBytes int64 = 64 << 20

func main() {
	addr := flag.String("addr", env("BROKER_ADDR", "http://127.0.0.1:8090"), "broker HTTP address")
	token := flag.String("token", env("BROKER_TOKEN", ""), "optional bearer token")
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
		os.Exit(2)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	cmd := flag.Arg(0)
	var err error
	switch cmd {
	case "tui":
		app := NewTUIApp(*addr, *token)
		err = app.Run(context.Background())
	case "stats":
		err = request(client, *addr, *token, http.MethodGet, "/stats", nil)
	case "queues":
		err = request(client, *addr, *token, http.MethodGet, "/queues", nil)
	case "topics":
		err = request(client, *addr, *token, http.MethodGet, "/topics", nil)
	case "streams":
		err = request(client, *addr, *token, http.MethodGet, "/streams", nil)
	case "stream":
		if flag.NArg() < 2 {
			die("stream requires stream name")
		}
		err = request(client, *addr, *token, http.MethodGet, "/streams/"+flag.Arg(1)+"/messages", nil)
	case "dlq":
		if flag.NArg() < 2 {
			err = request(client, *addr, *token, http.MethodGet, "/dlq", nil)
		} else {
			err = request(client, *addr, *token, http.MethodGet, "/dlq/"+flag.Arg(1)+"/messages", nil)
		}
	case "dlq-dashboard":
		err = request(client, *addr, *token, http.MethodGet, "/dlq/dashboard", nil)
	case "dlq-errors":
		if flag.NArg() < 2 {
			die("dlq-errors requires queue name")
		}
		err = request(client, *addr, *token, http.MethodGet, "/dlq/"+flag.Arg(1)+"/errors", nil)
	case "dlq-replay":
		if flag.NArg() < 2 {
			die("dlq-replay requires queue name")
		}
		err = request(client, *addr, *token, http.MethodPost, "/dlq/"+flag.Arg(1)+"/replay-all", nil)
	case "dlq-archive":
		if flag.NArg() < 2 {
			die("dlq-archive requires queue name")
		}
		err = request(client, *addr, *token, http.MethodPost, "/dlq/"+flag.Arg(1)+"/archive", nil)
	case "dlq-export":
		if flag.NArg() < 2 {
			die("dlq-export requires queue name")
		}
		err = request(client, *addr, *token, http.MethodGet, "/dlq/"+flag.Arg(1)+"/export", nil)
	case "reload":
		err = request(client, *addr, *token, http.MethodPost, "/config/reload", nil)
	case "pull":
		if flag.NArg() < 2 {
			die("pull requires queue name")
		}
		err = pull(client, *addr, *token, flag.Arg(1), flag.Args()[2:])
	case "ack":
		if flag.NArg() < 2 {
			die("ack requires message id")
		}
		err = ack(client, *addr, *token, flag.Arg(1), flag.Args()[2:])
	case "messages":
		if flag.NArg() < 2 {
			die("messages requires queue name")
		}
		err = request(client, *addr, *token, http.MethodGet, "/queues/"+flag.Arg(1)+"/messages", nil)
	case "pause":
		if flag.NArg() < 2 {
			die("pause requires queue name")
		}
		err = request(client, *addr, *token, http.MethodPost, "/queues/"+flag.Arg(1)+"/pause", nil)
	case "resume":
		if flag.NArg() < 2 {
			die("resume requires queue name")
		}
		err = request(client, *addr, *token, http.MethodPost, "/queues/"+flag.Arg(1)+"/resume", nil)
	case "purge":
		if flag.NArg() < 2 {
			die("purge requires queue name")
		}
		err = request(client, *addr, *token, http.MethodPost, "/queues/"+flag.Arg(1)+"/purge", nil)
	case "publish":
		err = publish(client, *addr, *token, flag.Args()[1:])
	case "move":
		err = move(client, *addr, *token, flag.Args()[1:])
	case "consumers":
		err = consumers(client, *addr, *token, flag.Args()[1:])
	case "sagas":
		if flag.NArg() < 2 {
			err = request(client, *addr, *token, http.MethodGet, "/v1/sagas", nil)
		} else {
			err = request(client, *addr, *token, http.MethodGet, "/v1/sagas/"+flag.Arg(1), nil)
		}
	case "cluster":
		err = clusterCommand(client, *addr, *token, flag.Args()[1:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		die(err.Error())
	}
}

func clusterCommand(client *http.Client, addr, token string, args []string) error {
	if len(args) == 0 || args[0] == "status" {
		return request(client, addr, token, http.MethodGet, "/cluster/status", nil)
	}
	switch args[0] {
	case "nodes":
		if len(args) != 1 {
			return fmt.Errorf("cluster nodes takes no arguments")
		}
		return request(client, addr, token, http.MethodGet, "/cluster/nodes", nil)
	case "upgrade-plan":
		if len(args) != 2 {
			return fmt.Errorf("cluster upgrade-plan requires target version")
		}
		path := "/cluster/upgrade-plan?target_version=" + url.QueryEscape(args[1])
		return request(client, addr, token, http.MethodGet, path, nil)
	case "removal-plan", "drain-start", "drain-complete":
		if len(args) != 2 {
			return fmt.Errorf("cluster %s requires peer ID", args[0])
		}
		peerID, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil || peerID == 0 {
			return fmt.Errorf("cluster %s requires a positive numeric peer ID", args[0])
		}
		path := "/cluster/nodes/" + strconv.FormatUint(peerID, 10)
		switch args[0] {
		case "removal-plan":
			return request(client, addr, token, http.MethodGet, path+"/removal-plan", nil)
		case "drain-start":
			return request(client, addr, token, http.MethodPost, path+"/drain", map[string]any{})
		default:
			return request(client, addr, token, http.MethodPost, path+"/drain/complete", map[string]any{})
		}
	case "leadership-transfer":
		if len(args) != 2 {
			return fmt.Errorf("cluster leadership-transfer requires peer ID")
		}
		peerID, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil || peerID == 0 {
			return fmt.Errorf("cluster leadership-transfer requires a positive numeric peer ID")
		}
		return request(client, addr, token, http.MethodPost, "/cluster/leadership", map[string]uint64{"peer_id": peerID})
	default:
		return fmt.Errorf("unknown cluster command %q", args[0])
	}
}

func consumers(client *http.Client, addr, token string, args []string) error {
	if len(args) == 0 {
		return request(client, addr, token, http.MethodGet, "/v1/consumers", nil)
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "ban":
		if len(rest) < 1 {
			return fmt.Errorf("ban requires consumer id")
		}
		reason := "banned via brokerctl"
		if len(rest) >= 2 {
			reason = rest[1]
		}
		return request(client, addr, token, http.MethodPost, "/v1/consumers/"+rest[0]+"/ban?reason="+reason, nil)
	case "unban":
		if len(rest) < 1 {
			return fmt.Errorf("unban requires consumer id")
		}
		return request(client, addr, token, http.MethodPost, "/v1/consumers/"+rest[0]+"/unban", nil)
	default:
		return request(client, addr, token, http.MethodGet, "/v1/consumers/"+sub, nil)
	}
}

func publish(client *http.Client, addr, token string, args []string) error {
	fs := flag.NewFlagSet("publish", flag.ExitOnError)
	topic := fs.String("topic", "", "topic")
	queue := fs.String("queue", "", "queue")
	typ := fs.String("type", "", "message type")
	stream := fs.String("stream", "", "stream")
	payload := fs.String("payload", `{}`, "JSON payload")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var raw json.RawMessage = json.RawMessage(*payload)
	body := map[string]any{"topic": *topic, "queue": *queue, "stream": *stream, "type": *typ, "payload": raw}
	return request(client, addr, token, http.MethodPost, "/publish", body)
}

func pull(client *http.Client, addr, token, queue string, args []string) error {
	fs := flag.NewFlagSet("pull", flag.ExitOnError)
	consumer := fs.String("consumer", "brokerctl", "consumer id")
	group := fs.String("group", "brokerctl", "consumer group")
	limit := fs.Int("limit", 1, "number of messages")
	if err := fs.Parse(args); err != nil {
		return err
	}
	body := map[string]any{"consumer_id": *consumer, "consumer_group": *group, "limit": *limit}
	return request(client, addr, token, http.MethodPost, "/queues/"+queue+"/pull", body)
}

func ack(client *http.Client, addr, token, id string, args []string) error {
	fs := flag.NewFlagSet("ack", flag.ExitOnError)
	queue := fs.String("queue", "", "queue name")
	lease := fs.String("lease", "", "lease token")
	consumer := fs.String("consumer", "brokerctl", "consumer id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	body := map[string]any{"queue": *queue, "lease_token": *lease, "consumer_id": *consumer}
	return request(client, addr, token, http.MethodPost, "/messages/"+id+"/ack", body)
}

func move(client *http.Client, addr, token string, args []string) error {
	fs := flag.NewFlagSet("move", flag.ExitOnError)
	fromType := fs.String("from-type", "queue", "source type: queue/topic/stream/dlq/archive")
	from := fs.String("from", "", "source name")
	toType := fs.String("to-type", "queue", "destination type")
	to := fs.String("to", "", "destination name")
	mode := fs.String("mode", "move", "move/copy/replay/reroute/quarantine/park/archive")
	ids := fs.String("ids", "", "comma separated message ids")
	if err := fs.Parse(args); err != nil {
		return err
	}
	body := map[string]any{"source_type": *fromType, "source": *from, "destination_type": *toType, "destination": *to, "mode": *mode}
	if *ids != "" {
		body["message_ids"] = strings.Split(*ids, ",")
	}
	return request(client, addr, token, http.MethodPost, "/move", body)
}

func request(client *http.Client, addr, token, method, path string, body any) error {
	var r io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, strings.TrimRight(addr, "/")+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, maxBrokerctlResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read broker response: %w", err)
	}
	if int64(len(out)) > maxBrokerctlResponseBytes {
		return fmt.Errorf("broker response exceeds %d bytes", maxBrokerctlResponseBytes)
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(out)))
	}
	fmt.Print(string(out))
	return nil
}

func usage() {
	fmt.Println(`brokerctl commands:
  tui (interactive full-screen console)
  stats
  queues
  topics
  streams
  stream <stream>
  dlq [queue]
  dlq-dashboard
  dlq-errors <queue>
  dlq-replay <queue>
  dlq-archive <queue>
  dlq-export <queue>
  reload (trigger dynamic config hot-reload)
  pull <queue> [-consumer id] [-group group] [-limit n]
  ack <message-id> -queue <queue> -lease <lease-token>
  messages <queue>
  pause <queue>
  resume <queue>
  purge <queue>
  publish -topic <topic> -queue <queue> -stream <stream> -type <type> -payload '<json>'
  move -from-type queue -from <name> -to-type queue -to <name> [-mode move] [-ids id1,id2]
  consumers [id|ban <id> [reason]|unban <id>]
  sagas [id]
  cluster [status|nodes|upgrade-plan <target-version>|removal-plan <peer-id>|drain-start <peer-id>|drain-complete <peer-id>|leadership-transfer <peer-id>]`)
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
