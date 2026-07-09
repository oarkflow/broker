package broker

import (
	"context"
	"errors"
	"testing"
)

func TestMemorySchemaRegistryValidatesRequiredJSONFields(t *testing.T) {
	reg := NewMemorySchemaRegistry(SchemaDefinition{Name: "invoice.created", Version: "v1", Required: []string{"invoice_id", "amount"}})
	b := MustNew(Config{SchemaRegistry: reg})
	defer b.Stop(context.Background())
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := b.Publisher().Publish(context.Background(), "invoice.created", map[string]any{"invoice_id": "inv-1"}, WithSchema("invoice.created", "v1"))
	if err == nil || !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("missing required field err=%v", err)
	}
	_, err = b.Publisher().Publish(context.Background(), "invoice.created", map[string]any{"invoice_id": "inv-1", "amount": 1499}, WithSchema("invoice.created", "v1"))
	if err != nil {
		t.Fatalf("valid payload: %v", err)
	}
}
