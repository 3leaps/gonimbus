package indexreader

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

const (
	BridgeIdentitySchema  = "gonimbus.index_set_identity.canonical-json-lf.v1"
	BridgeIdentityProfile = "default"
)

// CanonicalIdentityAuthority supplies exactly one identity authority. The
// canonical form is the exact payload JSON plus one LF. A declaration is the
// closed typed reconstruction input; it is never itself published.
type CanonicalIdentityAuthority struct {
	CanonicalJSONLF []byte
	DeclarationJSON []byte
}

type bridgeIdentity struct {
	bytes  []byte
	sha256 string
}

type identityDeclaration struct {
	Type            string                   `json:"type"`
	BaseURI         string                   `json:"base_uri"`
	Provider        string                   `json:"provider"`
	StorageProvider string                   `json:"storage_provider,omitempty"`
	CloudProvider   string                   `json:"cloud_provider,omitempty"`
	RegionKind      string                   `json:"region_kind,omitempty"`
	Region          string                   `json:"region,omitempty"`
	EndpointHost    string                   `json:"endpoint_host,omitempty"`
	Build           identityDeclarationBuild `json:"build"`
	PathDate        *identityDeclarationPath `json:"path_date,omitempty"`
}

type identityDeclarationBuild struct {
	SourceType      string    `json:"source_type"`
	SchemaVersion   *int      `json:"schema_version"`
	GonimbusVersion string    `json:"gonimbus_version,omitempty"`
	Includes        *[]string `json:"includes"`
	Excludes        []string  `json:"excludes,omitempty"`
	IncludeHidden   *bool     `json:"include_hidden"`
	FiltersHash     string    `json:"filters_hash,omitempty"`
	ScopeHash       string    `json:"scope_hash,omitempty"`
}

type identityDeclarationPath struct {
	Method       string `json:"method"`
	Regex        string `json:"regex,omitempty"`
	SegmentIndex *int   `json:"segment_index,omitempty"`
}

func resolveBridgeIdentity(
	authority CanonicalIdentityAuthority,
	expectedIndexSetID string,
	maxBytes int64,
) (bridgeIdentity, error) {
	hasFile := len(authority.CanonicalJSONLF) != 0
	hasDeclaration := len(authority.DeclarationJSON) != 0
	if hasFile == hasDeclaration {
		return bridgeIdentity{}, newBridgeError(BridgeErrorIdentityInvalid)
	}
	if hasFile {
		return validateCanonicalIdentityBytes(authority.CanonicalJSONLF, expectedIndexSetID, maxBytes)
	}
	return reconstructCanonicalIdentity(authority.DeclarationJSON, expectedIndexSetID, maxBytes)
}

func validateCanonicalIdentityBytes(raw []byte, expectedIndexSetID string, maxBytes int64) (bridgeIdentity, error) {
	if int64(len(raw)) > maxBytes || len(raw) < 3 || raw[len(raw)-1] != '\n' ||
		raw[len(raw)-2] == '\n' || bytes.HasPrefix(raw, []byte{0xef, 0xbb, 0xbf}) {
		return bridgeIdentity{}, newBridgeError(BridgeErrorIdentityInvalid)
	}
	payloadBytes := raw[:len(raw)-1]
	var payload indexstore.IndexSetIdentityPayload
	if err := strictDecodeJSON(payloadBytes, &payload, true); err != nil {
		return bridgeIdentity{}, newBridgeError(BridgeErrorIdentityInvalid)
	}
	params, err := paramsFromIdentityPayload(payload)
	if err != nil {
		return bridgeIdentity{}, newBridgeError(BridgeErrorIdentityInvalid)
	}
	identity, err := indexstore.ComputeIndexSetID(params)
	if err != nil || identity.IndexSetID != expectedIndexSetID ||
		identity.CanonicalJSON != string(payloadBytes) {
		return bridgeIdentity{}, newBridgeError(BridgeErrorIdentityInvalid)
	}
	sum := sha256.Sum256(raw)
	return bridgeIdentity{
		bytes:  append([]byte(nil), raw...),
		sha256: hex.EncodeToString(sum[:]),
	}, nil
}

func reconstructCanonicalIdentity(raw []byte, expectedIndexSetID string, maxBytes int64) (bridgeIdentity, error) {
	if len(raw) == 0 || int64(len(raw)) > maxBytes ||
		bytes.HasPrefix(raw, []byte{0xef, 0xbb, 0xbf}) {
		return bridgeIdentity{}, newBridgeError(BridgeErrorIdentityInvalid)
	}
	var declaration identityDeclaration
	if err := strictDecodeJSON(raw, &declaration, true); err != nil {
		return bridgeIdentity{}, newBridgeError(BridgeErrorIdentityInvalid)
	}
	params, err := paramsFromDeclaration(declaration)
	if err != nil {
		return bridgeIdentity{}, newBridgeError(BridgeErrorIdentityInvalid)
	}
	identity, err := indexstore.ComputeIndexSetID(params)
	if err != nil || identity.IndexSetID != expectedIndexSetID {
		return bridgeIdentity{}, newBridgeError(BridgeErrorIdentityInvalid)
	}
	canonical := append([]byte(identity.CanonicalJSON), '\n')
	sum := sha256.Sum256(canonical)
	return bridgeIdentity{bytes: canonical, sha256: hex.EncodeToString(sum[:])}, nil
}

func paramsFromDeclaration(d identityDeclaration) (indexstore.IndexSetParams, error) {
	if d.Type != "gonimbus.index_set_identity.declaration.v1" ||
		strings.TrimSpace(d.BaseURI) == "" ||
		strings.TrimSpace(d.Provider) == "" ||
		strings.TrimSpace(d.Build.SourceType) == "" ||
		d.Build.SchemaVersion == nil || *d.Build.SchemaVersion < 1 ||
		d.Build.Includes == nil || d.Build.IncludeHidden == nil {
		return indexstore.IndexSetParams{}, errors.New("invalid declaration")
	}
	params := indexstore.IndexSetParams{
		BaseURI:         strings.TrimSpace(d.BaseURI),
		Provider:        strings.TrimSpace(d.Provider),
		StorageProvider: strings.TrimSpace(d.StorageProvider),
		CloudProvider:   strings.TrimSpace(d.CloudProvider),
		RegionKind:      strings.TrimSpace(d.RegionKind),
		Region:          strings.TrimSpace(d.Region),
		EndpointHost:    strings.ToLower(strings.TrimSpace(d.EndpointHost)),
		BuildParams: indexstore.BuildParams{
			SourceType:      strings.TrimSpace(d.Build.SourceType),
			SchemaVersion:   *d.Build.SchemaVersion,
			GonimbusVersion: strings.TrimSpace(d.Build.GonimbusVersion),
			Includes:        append([]string(nil), (*d.Build.Includes)...),
			Excludes:        append([]string(nil), d.Build.Excludes...),
			IncludeHidden:   *d.Build.IncludeHidden,
			FiltersHash:     strings.TrimSpace(d.Build.FiltersHash),
			ScopeHash:       strings.TrimSpace(d.Build.ScopeHash),
		},
	}
	if d.PathDate != nil {
		pathDate, err := declarationPathDate(*d.PathDate)
		if err != nil {
			return indexstore.IndexSetParams{}, err
		}
		params.BuildParams.PathDateExtraction = pathDate
	}
	return params, nil
}

func declarationPathDate(path identityDeclarationPath) (*indexstore.PathDateExtraction, error) {
	method := strings.TrimSpace(path.Method)
	switch method {
	case "regex":
		regex := strings.TrimSpace(path.Regex)
		if regex == "" || path.SegmentIndex != nil {
			return nil, errors.New("invalid declaration")
		}
		return &indexstore.PathDateExtraction{Method: method, Regex: regex}, nil
	case "segment":
		if path.SegmentIndex == nil || *path.SegmentIndex < 0 || strings.TrimSpace(path.Regex) != "" {
			return nil, errors.New("invalid declaration")
		}
		return &indexstore.PathDateExtraction{Method: method, SegmentIndex: *path.SegmentIndex}, nil
	default:
		return nil, errors.New("invalid declaration")
	}
}

func paramsFromIdentityPayload(payload indexstore.IndexSetIdentityPayload) (indexstore.IndexSetParams, error) {
	if strings.TrimSpace(payload.BaseURI) == "" ||
		strings.TrimSpace(payload.Provider) == "" ||
		strings.TrimSpace(payload.Build.SourceType) == "" ||
		payload.Build.SchemaVersion < 1 ||
		payload.Build.Includes == nil {
		return indexstore.IndexSetParams{}, errors.New("invalid identity")
	}
	params := indexstore.IndexSetParams{
		BaseURI:         payload.BaseURI,
		Provider:        payload.Provider,
		StorageProvider: payload.StorageProvider,
		CloudProvider:   payload.CloudProvider,
		RegionKind:      payload.RegionKind,
		Region:          payload.Region,
		EndpointHost:    payload.EndpointHost,
		BuildParams: indexstore.BuildParams{
			SourceType:      payload.Build.SourceType,
			SchemaVersion:   payload.Build.SchemaVersion,
			GonimbusVersion: payload.Build.GonimbusVersion,
			Includes:        append([]string(nil), payload.Build.Includes...),
			Excludes:        append([]string(nil), payload.Build.Excludes...),
			IncludeHidden:   payload.Build.IncludeHidden,
			FiltersHash:     payload.Build.FiltersHash,
			ScopeHash:       payload.Build.ScopeHash,
		},
	}
	if payload.PathDate != nil {
		switch payload.PathDate.Method {
		case "regex":
			if strings.TrimSpace(payload.PathDate.Regex) == "" || payload.PathDate.SegmentIndex != 0 {
				return indexstore.IndexSetParams{}, errors.New("invalid identity")
			}
		case "segment":
			if payload.PathDate.Regex != "" || payload.PathDate.SegmentIndex < 0 {
				return indexstore.IndexSetParams{}, errors.New("invalid identity")
			}
		default:
			return indexstore.IndexSetParams{}, errors.New("invalid identity")
		}
		params.BuildParams.PathDateExtraction = &indexstore.PathDateExtraction{
			Method:       payload.PathDate.Method,
			Regex:        payload.PathDate.Regex,
			SegmentIndex: payload.PathDate.SegmentIndex,
		}
	}
	return params, nil
}

func strictDecodeJSON(data []byte, dst any, rejectNull bool) error {
	if len(data) == 0 {
		return io.ErrUnexpectedEOF
	}
	scan := json.NewDecoder(bytes.NewReader(data))
	scan.UseNumber()
	if err := scanStrictJSONValue(scan, rejectNull); err != nil {
		return err
	}
	if _, err := scan.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON")
		}
		return err
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON")
		}
		return err
	}
	return nil
}

func scanStrictJSONValue(decoder *json.Decoder, rejectNull bool) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil && rejectNull {
		return errors.New("null is not allowed")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate object key")
			}
			seen[key] = struct{}{}
			if err := scanStrictJSONValue(decoder, rejectNull); err != nil {
				return err
			}
		}
		closeToken, err := decoder.Token()
		if err != nil || closeToken != json.Delim('}') {
			return errors.New("malformed JSON object")
		}
	case '[':
		for decoder.More() {
			if err := scanStrictJSONValue(decoder, rejectNull); err != nil {
				return err
			}
		}
		closeToken, err := decoder.Token()
		if err != nil || closeToken != json.Delim(']') {
			return errors.New("malformed JSON array")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}
