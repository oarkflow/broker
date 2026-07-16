package main

import (
	"context"
	"log"
	"time"

	"github.com/oarkflow/broker"
)

// Stage 9: MQTT Protocol
// Demonstrates the MQTT 3.1.1 protocol server.
// Connect with any MQTT client to localhost:1883

func main() {
	ctx := context.Background()

	b := broker.MustNew(broker.Config{
		Name:              "mqtt-example",
		DefaultQueue:      "default",
		DefaultVisibility: 30 * time.Second,
	})
	if err := b.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Stop(context.Background())

	// Subscribe to MQTT-style topics via broker
	_, err := b.Subscribe(ctx, broker.ConsumerConfig{
		Name:  "mqtt-subscriber",
		Queue: "mqtt.sub.home.sensors.temperature",
	}, func(ctx context.Context, d broker.Delivery) error {
		log.Printf("[subscriber] received: topic=%s payload=%s",
			d.Message.Headers["mqtt_topic"], string(d.Message.Payload))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Start MQTT server
	mqttServer := broker.NewMQTTServer(b, broker.MQTTConfig{
		ListenAddr:     ":1883",
		MaxConnections: 100,
		KeepAlive:      60,
	})
	go func() {
		log.Println("[mqtt] server starting on :1883")
		if err := mqttServer.ListenAndServe(); err != nil {
			log.Printf("[mqtt] server error: %v", err)
		}
	}()

	// Publish via broker (simulating MQTT publish)
	pub := b.Publisher()
	for i := 0; i < 5; i++ {
		_, _ = pub.Publish(ctx, "home.sensors.temperature", map[string]any{
			"sensor_id": "temp-01",
			"value":     22.5 + float64(i)*0.1,
			"unit":      "celsius",
		}, broker.WithHeaders(map[string]string{
			"mqtt_topic": "home/sensors/temperature",
			"mqtt_qos":   "0",
		}))
	}

	time.Sleep(3 * time.Second)
	log.Println("[result] MQTT server running, connect with any MQTT client")
}
