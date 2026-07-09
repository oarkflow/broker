package broker

import "encoding/json"

type Codec interface {
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
	ContentType() string
}

type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	return json.Marshal(v)
}
func (JSONCodec) Unmarshal(b []byte, v any) error {
	if p, ok := v.(*[]byte); ok {
		*p = append((*p)[:0], b...)
		return nil
	}
	return json.Unmarshal(b, v)
}
func (JSONCodec) ContentType() string { return "application/json" }

type RawCodec struct{}

func (RawCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	default:
		return json.Marshal(v)
	}
}
func (RawCodec) Unmarshal(b []byte, v any) error {
	if p, ok := v.(*[]byte); ok {
		*p = append((*p)[:0], b...)
		return nil
	}
	if p, ok := v.(*string); ok {
		*p = string(b)
		return nil
	}
	return json.Unmarshal(b, v)
}
func (RawCodec) ContentType() string { return "application/octet-stream" }
