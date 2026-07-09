package main

import (
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/broker"
	bclient "github.com/oarkflow/broker/pkg/client"
	"github.com/oarkflow/dagflow"
)

// deliveryCtxKey smuggles the broker Delivery for the message currently
// being processed into dagflow's ExecutionContext.Context, so individual
// pipeline nodes can report per-stage progress back to the broker while the
// workflow is still running (RunSync executes every node synchronously on
// this goroutine).
type deliveryCtxKey struct{}

func withDelivery(ctx context.Context, d bclient.Delivery) context.Context {
	return context.WithValue(ctx, deliveryCtxKey{}, d)
}

func reportProgress(ctx context.Context, percent float64, message string) {
	d, ok := ctx.Value(deliveryCtxKey{}).(bclient.Delivery)
	if !ok {
		return
	}
	_ = d.Progress(context.Background(), broker.Progress{Percent: percent, Message: message, UpdatedAt: time.Now()})
}

func asOrder(input any) (map[string]any, error) {
	order, ok := input.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected an order object, got %T", input)
	}
	return order, nil
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}

func validateOrder(ec *dagflow.ExecutionContext, input any) (any, error) {
	order, err := asOrder(input)
	if err != nil {
		return nil, dagflow.NewPermanentError("invalid order payload: %v", err)
	}
	orderID, _ := order["order_id"].(string)
	customerID, _ := order["customer_id"].(string)
	item, _ := order["item"].(string)
	quantity, _ := asFloat(order["quantity"])
	unitPrice, _ := asFloat(order["unit_price"])

	var missing []string
	if orderID == "" {
		missing = append(missing, "order_id")
	}
	if customerID == "" {
		missing = append(missing, "customer_id")
	}
	if item == "" {
		missing = append(missing, "item")
	}
	if quantity <= 0 {
		missing = append(missing, "quantity")
	}
	if unitPrice <= 0 {
		missing = append(missing, "unit_price")
	}
	if len(missing) > 0 {
		return nil, dagflow.NewPermanentError("order validation failed, missing/invalid fields: %v", missing)
	}

	order["quantity"] = quantity
	order["unit_price"] = unitPrice
	reportProgress(ec.Context, 20, "order validated")
	return order, nil
}

func enrichOrder(ec *dagflow.ExecutionContext, input any) (any, error) {
	order, err := asOrder(input)
	if err != nil {
		return nil, err
	}
	quantity, _ := asFloat(order["quantity"])
	unitPrice, _ := asFloat(order["unit_price"])
	order["total"] = quantity * unitPrice
	order["enriched_at"] = time.Now().UTC().Format(time.RFC3339)
	reportProgress(ec.Context, 45, fmt.Sprintf("order enriched total=%.2f", order["total"]))
	return order, nil
}

func reviewOrder(ec *dagflow.ExecutionContext, input any) (any, error) {
	order, err := asOrder(input)
	if err != nil {
		return nil, err
	}
	order["status"] = "pending_review"
	order["note"] = "order exceeds the auto-approval threshold and was routed to manual review"
	reportProgress(ec.Context, 70, "order routed to manual review")
	return order, nil
}

func fulfillOrder(ec *dagflow.ExecutionContext, input any) (any, error) {
	order, err := asOrder(input)
	if err != nil {
		return nil, err
	}
	order["status"] = "fulfilled"
	order["note"] = "order auto-fulfilled"
	reportProgress(ec.Context, 70, "order auto-fulfilled")
	return order, nil
}

func notifyOrder(ec *dagflow.ExecutionContext, input any) (any, error) {
	order, err := asOrder(input)
	if err != nil {
		return nil, err
	}
	order["notified_at"] = time.Now().UTC().Format(time.RFC3339)
	reportProgress(ec.Context, 100, "customer notified")
	return order, nil
}
