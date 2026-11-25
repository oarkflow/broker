package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/broker/cluster"
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

// ControllerSchemaRegistry validates against schema records stored in the
// distributed controller quorum. Reads are linearizable so every publisher
// observes committed schema changes after its quorum read point.
type ControllerSchemaRegistry struct {
	Controller *cluster.ControllerQuorum
}

func (r *ControllerSchemaRegistry) Register(definition SchemaDefinition) error {
	return r.RegisterContext(context.Background(), definition)
}

func (r *ControllerSchemaRegistry) RegisterContext(ctx context.Context, definition SchemaDefinition) error {
	if r == nil || r.Controller == nil {
		return fmt.Errorf("schema registry: controller quorum is unavailable")
	}
	if hasRestrictedTenantScope(ctx) {
		return errf(ErrUnauthorized, "tenant-scoped credentials cannot alter shared schema configuration")
	}
	return r.Controller.RegisterSchema(ctx, cluster.SchemaDefinition{
		Name: definition.Name, Version: definition.Version, ContentType: definition.ContentType,
		Required: append([]string(nil), definition.Required...), AllowUnknown: definition.AllowUnknown,
		CompatibleWith: definition.CompatibleWith, Deprecated: definition.Deprecated,
		Description: definition.Description,
	})
}

func (r *ControllerSchemaRegistry) Validate(ctx context.Context, msg Message) error {
	if msg.Schema == "" {
		return nil
	}
	if r == nil || r.Controller == nil {
		return fmt.Errorf("schema registry: controller quorum is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	version := msg.SchemaVersion
	if version == "" {
		version = "v1"
	}
	definition, err := r.Controller.GetSchemaLinearizable(ctx, msg.Schema, version)
	if err != nil {
		if errors.Is(err, cluster.ErrSchemaNotFound) {
			return errf(ErrInvalidMessage, "schema %s/%s is not registered", msg.Schema, version)
		}
		return err
	}
	return validateSchemaPayload(msg, SchemaDefinition{
		Name: definition.Name, Version: definition.Version, ContentType: definition.ContentType,
		Required: definition.Required, AllowUnknown: definition.AllowUnknown,
		CompatibleWith: definition.CompatibleWith, Deprecated: definition.Deprecated,
		Description: definition.Description,
	})
}

func validateSchemaPayload(msg Message, definition SchemaDefinition) error {
	if definition.Deprecated {
		return errf(ErrInvalidMessage, "schema %s/%s is deprecated", definition.Name, definition.Version)
	}
	if !strings.Contains(strings.ToLower(definition.ContentType), "json") {
		return nil
	}
	var obj map[string]any
	if err := json.Unmarshal(msg.Payload, &obj); err != nil {
		return errf(ErrInvalidMessage, "schema %s/%s JSON validation failed: %v", definition.Name, definition.Version, err)
	}
	for _, required := range definition.Required {
		if _, ok := obj[required]; !ok {
			return errf(ErrInvalidMessage, "schema %s/%s missing required field %s", definition.Name, definition.Version, required)
		}
	}
	return nil
}

func NewMemorySchemaRegistry(defs ...SchemaDefinition) *MemorySchemaRegistry {
	r := &MemorySchemaRegistry{schemas: map[string]SchemaDefinition{}, versions: map[string][]SchemaDefinition{}}
	for _, d := range defs {
		_ = r.Register(d)
	}
	return r
}

// Register adds or replaces a schema version. It is the unrestricted setup form;
// use RegisterContext when the caller is an authenticated principal.
func (r *MemorySchemaRegistry) Register(d SchemaDefinition) error {
	return r.RegisterContext(context.Background(), d)
}

// RegisterContext adds or replaces a schema version. Schemas are shared
// infrastructure, so tenant-scoped credentials may read but not mutate them,
// matching the queue/topic/stream declaration rules.
func (r *MemorySchemaRegistry) RegisterContext(ctx context.Context, d SchemaDefinition) error {
	if hasRestrictedTenantScope(ctx) {
		return errf(ErrUnauthorized, "tenant-scoped credentials cannot alter shared schema configuration")
	}
	if d.Name == "" {
		return errf(ErrInvalidMessage, "schema name required")
	}
	if d.Version == "" {
		d.Version = "v1"
	}
	if d.ContentType == "" {
		d.ContentType = "application/json"
	}
	d = cloneSchemaDefinition(d)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.schemas == nil {
		r.schemas = make(map[string]SchemaDefinition)
	}
	if r.versions == nil {
		r.versions = make(map[string][]SchemaDefinition)
	}
	key := schemaKey(d.Name, d.Version)
	if existing, ok := r.schemas[key]; ok {
		if existing.Deprecated {
			return errf(ErrInvalidMessage, "cannot re-register deprecated schema %s/%s", d.Name, d.Version)
		}
		for i := range r.versions[d.Name] {
			if r.versions[d.Name][i].Version == d.Version {
				r.versions[d.Name][i] = d
				break
			}
		}
	} else {
		r.versions[d.Name] = append(r.versions[d.Name], d)
	}
	r.schemas[key] = d
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
	return validateSchemaPayload(msg, d)
}

// GetSchema returns the schema definition for the given name and version.
func (r *MemorySchemaRegistry) GetSchema(name, version string) (SchemaDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.schemas[schemaKey(name, version)]
	return cloneSchemaDefinition(d), ok
}

// ListVersions returns all registered versions for a schema name.
func (r *MemorySchemaRegistry) ListVersions(name string) []SchemaDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions := r.versions[name]
	result := make([]SchemaDefinition, len(versions))
	for i := range versions {
		result[i] = cloneSchemaDefinition(versions[i])
	}
	return result
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
	for i := range r.versions[name] {
		if r.versions[name][i].Version == version {
			r.versions[name][i] = cloneSchemaDefinition(d)
			break
		}
	}
	return nil
}

func cloneSchemaDefinition(d SchemaDefinition) SchemaDefinition {
	d.Required = append([]string(nil), d.Required...)
	return d
}

func schemaKey(name, version string) string {
	return fmt.Sprintf("%d:%s%s", len(name), name, version)
}
