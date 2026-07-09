package broker

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
)

// SchemaRegistry validates producer payloads before they enter the broker. It is
// deliberately small and dependency-free; production deployments can replace it
// with a JSON Schema, Protobuf, Avro, or external registry implementation.
type SchemaRegistry interface {
	Register(SchemaDefinition) error
	Validate(context.Context, Message) error
}

type SchemaDefinition struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	ContentType  string   `json:"content_type"`
	Required     []string `json:"required,omitempty"`
	AllowUnknown bool     `json:"allow_unknown"`
}

type MemorySchemaRegistry struct {
	mu      sync.RWMutex
	schemas map[string]SchemaDefinition
}

func NewMemorySchemaRegistry(defs ...SchemaDefinition) *MemorySchemaRegistry {
	r := &MemorySchemaRegistry{schemas: map[string]SchemaDefinition{}}
	for _, d := range defs {
		_ = r.Register(d)
	}
	return r
}

func (r *MemorySchemaRegistry) Register(d SchemaDefinition) error {
	if d.Name == "" {
		return errf(ErrInvalidMessage, "schema name required")
	}
	if d.Version == "" {
		d.Version = "v1"
	}
	if d.ContentType == "" {
		d.ContentType = "application/json"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[schemaKey(d.Name, d.Version)] = d
	return nil
}

func (r *MemorySchemaRegistry) Validate(ctx context.Context, msg Message) error {
	if r == nil || msg.Schema == "" {
		return nil
	}
	version := msg.SchemaVersion
	if version == "" {
		version = "v1"
	}
	r.mu.RLock()
	d, ok := r.schemas[schemaKey(msg.Schema, version)]
	r.mu.RUnlock()
	if !ok {
		return errf(ErrInvalidMessage, "schema %s/%s is not registered", msg.Schema, version)
	}
	if strings.Contains(strings.ToLower(d.ContentType), "json") {
		var obj map[string]any
		if err := json.Unmarshal(msg.Payload, &obj); err != nil {
			return errf(ErrInvalidMessage, "schema %s/%s JSON validation failed: %v", msg.Schema, version, err)
		}
		for _, k := range d.Required {
			if _, ok := obj[k]; !ok {
				return errf(ErrInvalidMessage, "schema %s/%s missing required field %s", msg.Schema, version, k)
			}
		}
	}
	return nil
}

func schemaKey(name, version string) string { return name + "/" + version }
