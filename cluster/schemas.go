package cluster

import (
	"context"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxSchemaNameBytes    = 255
	maxSchemaVersionBytes = 128
	maxSchemaFields       = 4096
	maxSchemaFieldBytes   = 255
)

func schemaMetadataKey(name, version string) string { return name + "\x00" + version }

func normalizeSchemaDefinition(definition SchemaDefinition) (SchemaDefinition, error) {
	if definition.Name != strings.TrimSpace(definition.Name) || definition.Version != strings.TrimSpace(definition.Version) {
		return SchemaDefinition{}, ErrInvalidSchema
	}
	definition.ContentType = strings.TrimSpace(definition.ContentType)
	definition.CompatibleWith = strings.TrimSpace(definition.CompatibleWith)
	if definition.Name == "" || len(definition.Name) > maxSchemaNameBytes || !utf8.ValidString(definition.Name) ||
		strings.ContainsAny(definition.Name, "\x00/\\\r\n") {
		return SchemaDefinition{}, ErrInvalidSchema
	}
	if definition.Version == "" {
		definition.Version = "v1"
	}
	if len(definition.Version) > maxSchemaVersionBytes || !utf8.ValidString(definition.Version) || strings.ContainsAny(definition.Version, "\x00/\\\r\n") {
		return SchemaDefinition{}, ErrInvalidSchema
	}
	if definition.ContentType == "" {
		definition.ContentType = "application/json"
	}
	if len(definition.ContentType) > 255 || !utf8.ValidString(definition.ContentType) || strings.ContainsAny(definition.ContentType, "\r\n") {
		return SchemaDefinition{}, ErrInvalidSchema
	}
	if len(definition.CompatibleWith) > maxSchemaVersionBytes || !utf8.ValidString(definition.CompatibleWith) {
		return SchemaDefinition{}, ErrInvalidSchema
	}
	if len(definition.Description) > 4096 || !utf8.ValidString(definition.Description) {
		return SchemaDefinition{}, ErrInvalidSchema
	}
	if len(definition.Required) > maxSchemaFields {
		return SchemaDefinition{}, ErrInvalidSchema
	}
	definition.Required = append([]string(nil), definition.Required...)
	for i := range definition.Required {
		field := strings.TrimSpace(definition.Required[i])
		if field == "" || len(field) > maxSchemaFieldBytes || !utf8.ValidString(field) || field != definition.Required[i] {
			return SchemaDefinition{}, ErrInvalidSchema
		}
	}
	sort.Strings(definition.Required)
	for i := 1; i < len(definition.Required); i++ {
		if definition.Required[i] == definition.Required[i-1] {
			return SchemaDefinition{}, ErrInvalidSchema
		}
	}
	return definition, nil
}

func cloneSchemaMetadata(definition *SchemaDefinition) SchemaDefinition {
	copyDefinition := *definition
	copyDefinition.Required = append([]string(nil), definition.Required...)
	return copyDefinition
}

// RegisterSchema replicates a schema definition through the controller quorum.
// Re-registering a live version replaces its definition while retaining the
// original registration time. Deprecated versions are immutable.
func (cq *ControllerQuorum) RegisterSchema(ctx context.Context, definition SchemaDefinition) error {
	normalized, err := normalizeSchemaDefinition(definition)
	if err != nil || normalized.Deprecated {
		return ErrInvalidSchema
	}
	normalized.RegisteredAt = time.Time{}
	return cq.propose(ctx, MetadataCommand{Op: OpRegisterSchema, Schema: &normalized})
}

// DeprecateSchema prevents future registration and publication using a schema
// version while retaining the replicated definition for inspection.
func (cq *ControllerQuorum) DeprecateSchema(ctx context.Context, name, version string) error {
	name = strings.TrimSpace(name)
	version = strings.TrimSpace(version)
	if name == "" || version == "" || len(name) > maxSchemaNameBytes || len(version) > maxSchemaVersionBytes ||
		!utf8.ValidString(name) || !utf8.ValidString(version) {
		return ErrInvalidSchema
	}
	return cq.propose(ctx, MetadataCommand{Op: OpDeprecateSchema, SchemaName: name, SchemaVersion: version})
}

func (cq *ControllerQuorum) GetSchema(name, version string) (SchemaDefinition, error) {
	if version == "" {
		version = "v1"
	}
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	definition := cq.meta.Schemas[schemaMetadataKey(name, version)]
	if definition == nil {
		return SchemaDefinition{}, ErrSchemaNotFound
	}
	return cloneSchemaMetadata(definition), nil
}

func (cq *ControllerQuorum) GetSchemaLinearizable(ctx context.Context, name, version string) (SchemaDefinition, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return SchemaDefinition{}, err
	}
	return cq.GetSchema(name, version)
}

func (cq *ControllerQuorum) ListSchemas(name string) []SchemaDefinition {
	cq.meta.mu.RLock()
	defer cq.meta.mu.RUnlock()
	out := make([]SchemaDefinition, 0, len(cq.meta.Schemas))
	for _, definition := range cq.meta.Schemas {
		if name == "" || definition.Name == name {
			out = append(out, cloneSchemaMetadata(definition))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].Version < out[j].Version
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (cq *ControllerQuorum) ListSchemasLinearizable(ctx context.Context, name string) ([]SchemaDefinition, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cq.controller.ReadIndex(ctx); err != nil {
		return nil, err
	}
	return cq.ListSchemas(name), nil
}
