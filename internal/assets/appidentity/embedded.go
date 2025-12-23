package appidentityassets

import _ "embed"

// YAML is the embedded copy of `.fulmen/app.yaml`, mirrored into a Go-embeddable
// location for standalone binary behavior.
//
// It is kept in sync via `make sync-embedded-identity`.
//
//go:embed app.yaml
var YAML []byte
