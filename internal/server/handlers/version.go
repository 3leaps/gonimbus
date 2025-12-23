package handlers

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"runtime"

	"github.com/fulmenhq/gofulmen/appidentity"
	"github.com/fulmenhq/gofulmen/crucible"
)

// AppVersion is injected from main via SetVersionInfo
var (
	AppVersion   = "dev"
	AppCommit    = "unknown"
	AppBuildDate = "unknown"
	appIdentity  *appidentity.Identity
)

// SetVersionInfo sets the version information for the handler
func SetVersionInfo(version, commit, buildDate string) {
	AppVersion = version
	AppCommit = commit
	AppBuildDate = buildDate
}

// SetAppIdentity sets the app identity for the handler
func SetAppIdentity(identity *appidentity.Identity) {
	appIdentity = identity
}

// VersionResponse represents the version information response
type VersionResponse struct {
	App          AppInfo     `json:"app"`
	Dependencies DepInfo     `json:"dependencies"`
	Runtime      RuntimeInfo `json:"runtime"`
}

// AppInfo contains application version details
type AppInfo struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Commit    string `json:"git_commit"`
	BuildDate string `json:"build_date"`
	GoVersion string `json:"go_version,omitempty"`
}

// DepInfo contains dependency version information
type DepInfo struct {
	Gofulmen string `json:"gofulmen"`
	Crucible string `json:"crucible"`
}

// RuntimeInfo contains runtime environment information
type RuntimeInfo struct {
	Platform      string `json:"platform"`
	NumCPU        int    `json:"num_cpu"`
	NumGoroutines int    `json:"num_goroutines"`
}

// VersionHandler handles version information requests
func VersionHandler(w http.ResponseWriter, r *http.Request) {
	version := crucible.GetVersion()

	// Use app identity if set, otherwise fallback to the executable name.
	identity := appIdentity
	if identity == nil {
		fallbackName := "unknown"
		if len(os.Args) > 0 && os.Args[0] != "" {
			fallbackName = filepath.Base(os.Args[0])
		}
		identity = &appidentity.Identity{
			BinaryName: fallbackName,
		}
	}

	response := VersionResponse{
		App: AppInfo{
			Name:      identity.BinaryName,
			Version:   AppVersion,
			Commit:    AppCommit,
			BuildDate: AppBuildDate,
			GoVersion: runtime.Version(),
		},
		Dependencies: DepInfo{
			Gofulmen: version.Gofulmen,
			Crucible: version.Crucible,
		},
		Runtime: RuntimeInfo{
			Platform:      runtime.GOOS + "/" + runtime.GOARCH,
			NumCPU:        runtime.NumCPU(),
			NumGoroutines: runtime.NumGoroutine(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}
