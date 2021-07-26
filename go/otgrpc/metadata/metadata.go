package metadata

import (
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/net/context"
)

const (
	binHdrSuffix = "-bin"
)

// encodeKeyValue encodes key and value qualified for transmission via gRPC.
// Transmitting binary headers violates HTTP/2 spec.
// TODO(zhaoq): Maybe check if k is ASCII also.
func encodeKeyValue(k, v string) (string, string) {
	k = strings.ToLower(k)
	if strings.HasSuffix(k, binHdrSuffix) {
		val := base64.StdEncoding.EncodeToString([]byte(v))
		v = string(val)
	}
	return k, v
}

// DecodeKeyValue returns the original key and value corresponding to the
// encoded data in k, v.
// If k is a binary header and v contains comma, v is split on comma before decoded,
// and the decoded v will be joined with comma before returned.
func DecodeKeyValue(k, v string) (string, string, error) {
	if !strings.HasSuffix(k, binHdrSuffix) {
		return k, v, nil
	}
	vvs := strings.Split(v, ",")
	for i, vv := range vvs {
		val, err := base64.StdEncoding.DecodeString(vv)
		if err != nil {
			return "", "", err
		}
		vvs[i] = string(val)
	}
	return k, strings.Join(vvs, ","), nil
}

// MD is a mapping from metadata keys to values. Users should use the following
// two convenience functions New and Pairs to generate MD.
type MD map[string][]string

// New creates a MD from given key-value map.
func New(m map[string]string) MD {
	md := MD{}
	for k, v := range m {
		key, val := encodeKeyValue(k, v)
		md[key] = append(md[key], val)
	}
	return md
}

// Pairs returns an MD formed by the mapping of key, value ...
// Pairs panics if len(kv) is odd.
func Pairs(kv ...string) MD {
	if len(kv)%2 == 1 {
		panic(fmt.Sprintf("metadata: Pairs got the odd number of input pairs for metadata: %d", len(kv)))
	}
	md := MD{}
	var k string
	for i, s := range kv {
		if i%2 == 0 {
			k = s
			continue
		}
		key, val := encodeKeyValue(k, s)
		md[key] = append(md[key], val)
	}
	return md
}

// Len returns the number of items in md.
func (md MD) Len() int {
	return len(md)
}

// Copy returns a copy of md.
func (md MD) Copy() MD {
	return Join(md)
}

// Join joins any number of MDs into a single MD.
// The order of values for each key is determined by the order in which
// the MDs containing those values are presented to Join.
func Join(mds ...MD) MD {
	out := MD{}
	for _, md := range mds {
		for k, v := range md {
			out[k] = append(out[k], v...)
		}
	}
	return out
}

type mdKey struct{}

// NewContext creates a new context with md attached.
func NewContext(ctx context.Context, md MD) context.Context {
	return context.WithValue(ctx, mdKey{}, md)
}

// FromContext returns the MD in ctx if it exists.
func FromContext(ctx context.Context) (md MD, ok bool) {
	md, ok = ctx.Value(mdKey{}).(MD)
	return
}
