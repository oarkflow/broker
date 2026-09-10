package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// SchemaRegistry validates producer payloads before they enter the broker. It is
// deliberately small and dependency-free; production deployments can replace it
// with a JSON Schema, Protobuf, Avro, or external registry implementation.
type SchemaRegistry interface {
	Register(SchemaDefinition) error
	Validate(context.Context, Message) error
}

type SchemaDefinition struct {
	Name           string   `json:"name"`
	Version        string   `json:"version"`
	ContentType    string   `json:"content_type"`
	Required       []string `json:"required,omitempty"`
	AllowUnknown   bool     `json:"allow_unknown"`
	CompatibleWith string   `json:"compatible_with,omitempty"` // previous version this is compatible with
	Deprecated     bool     `json:"deprecated,omitempty"`
	Description    string   `json:"description,omitempty"`
}

// SchemaCompatibility represents the compatibility level between two schema versions.
type SchemaCompatibility string

const (
	SchemaCompatible    SchemaCompatibility = "compatible"
	SchemaIncompatible  SchemaCompatibility = "incompatible"
	SchemaForwardCompat SchemaCompatibility = "forward_compatible"
)

// VersionedSchema stores a schema version and its metadata.
type VersionedSchema struct {
	Definition   SchemaDefinition `json:"definition"`
	RegisteredAt time.Time        `json:"registered_at"`
}

type MemorySchemaRegistry struct {
	mu       sync.RWMutex
	schemas  map[string]SchemaDefinition
	versions map[string][]SchemaDefinition // name -> ordered versions
}

func NewMemorySchemaRegistry(defs ...SchemaDefinition) *MemorySchemaRegistry {
	r := &MemorySchemaRegistry{schemas: map[string]SchemaDefinition{}, versions: map[string][]SchemaDefinition{}}
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
	key := schemaKey(d.Name, d.Version)
	if existing, ok := r.schemas[key]; ok {
		if existing.Deprecated {
			return errf(ErrInvalidMessage, "cannot re-register deprecated schema %s/%s", d.Name, d.Version)
		}
	}
	r.schemas[key] = d
	r.versions[d.Name] = append(r.versions[d.Name], d)
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
	if d.Deprecated {
		return errf(ErrInvalidMessage, "schema %s/%s is deprecated", msg.Schema, version)
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

// GetSchema returns the schema definition for the given name and version.
func (r *MemorySchemaRegistry) GetSchema(name, version string) (SchemaDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.schemas[schemaKey(name, version)]
	return d, ok
}

// ListVersions returns all registered versions for a schema name.
func (r *MemorySchemaRegistry) ListVersions(name string) []SchemaDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.versions[name]
}

// CheckCompatibility checks if a new schema version is compatible with an existing version.
func (r *MemorySchemaRegistry) CheckCompatibility(name, newVersion, existingVersion string) SchemaCompatibility {
	r.mu.RLock()
	defer r.mu.RUnlock()
	newKey := schemaKey(name, newVersion)
	existingKey := schemaKey(name, existingVersion)
	newSchema, newOk := r.schemas[newKey]
	existingSchema, existingOk := r.schemas[existingKey]
	if !newOk || !existingOk {
		return SchemaIncompatible
	}
	if newSchema.CompatibleWith != "" && newSchema.CompatibleWith != existingVersion {
		return SchemaIncompatible
	}
	if len(newSchema.Required) < len(existingSchema.Required) {
		return SchemaIncompatible
	}
	for _, field := range existingSchema.Required {
		found := false
		for _, newField := range newSchema.Required {
			if field == newField {
				found = true
				break
			}
		}
		if !found {
			return SchemaIncompatible
		}
	}
	return SchemaCompatible
}

// LatestVersion returns the latest version for a schema name.
func (r *MemorySchemaRegistry) LatestVersion(name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions := r.versions[name]
	if len(versions) == 0 {
		return "", false
	}
	return versions[len(versions)-1].Version, true
}

// DeprecateSchema marks a schema version as deprecated.
func (r *MemorySchemaRegistry) DeprecateSchema(name, version string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := schemaKey(name, version)
	d, ok := r.schemas[key]
	if !ok {
		return fmt.Errorf("schema %s/%s not found", name, version)
	}
	d.Deprecated = true
	r.schemas[key] = d
	return nil
}

func schemaKey(name, version string) string { return name + "/" + version }
