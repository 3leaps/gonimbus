package cmd

import (
	"encoding/json"
	"fmt"
	"runtime"
	"runtime/debug"
	"strconv"

	"github.com/fulmenhq/gofulmen/crucible"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/internal/buildinfo"
)

var (
	extended    bool
	versionJSON bool

	readBuildInfo = debug.ReadBuildInfo
)

type versionJSONOutput struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Commit      string `json:"commit"`
	BuiltAt     string `json:"built_at"`
	Go          string `json:"go"`
	Platform    string `json:"platform"`
	OS          string `json:"os,omitempty"`
	Arch        string `json:"arch,omitempty"`
	Gofulmen    string `json:"gofulmen,omitempty"`
	Crucible    string `json:"crucible,omitempty"`
	ModulePath  string `json:"module_path,omitempty"`
	VCSRevision string `json:"vcs_revision,omitempty"`
	VCSTime     string `json:"vcs_time,omitempty"`
	VCSModified *bool  `json:"vcs_modified,omitempty"`
}

func effectiveVersionInfo() (version, commit, buildDate string) {
	if versionInfo.Version != "" || versionInfo.Commit != "" || versionInfo.BuildDate != "" {
		v := versionInfo.Version
		if v == "" {
			v = "dev"
		}
		c := versionInfo.Commit
		if c == "" {
			c = "unknown"
		}
		b := versionInfo.BuildDate
		if b == "" {
			b = "unknown"
		}
		return v, c, b
	}
	return buildinfo.Resolve("dev", "unknown", "unknown")
}

func effectiveBuildProvenance() (version, commit string, vcsModified *bool) {
	return buildinfo.ResolveProvenance(versionInfo.Version, versionInfo.Commit)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Long:  "Print version information. Use --extended for full details including Crucible and Go versions.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		identity := GetAppIdentity()
		binaryName := "gonimbus"
		if identity != nil && identity.BinaryName != "" {
			binaryName = identity.BinaryName
		}
		ver, commit, buildDate := effectiveVersionInfo()
		out := cmd.OutOrStdout()

		if versionJSON {
			payload := versionJSONOutput{
				Name:     binaryName,
				Version:  ver,
				Commit:   commit,
				BuiltAt:  buildDate,
				Go:       runtime.Version(),
				Platform: runtime.GOOS + "/" + runtime.GOARCH,
			}
			if extended {
				crucibleVer := crucible.GetVersion()
				payload.OS = runtime.GOOS
				payload.Arch = runtime.GOARCH
				payload.Gofulmen = crucibleVer.Gofulmen
				payload.Crucible = crucibleVer.Crucible
				if bi, ok := readBuildInfo(); ok && bi != nil {
					if bi.Main.Path != "" {
						payload.ModulePath = bi.Main.Path
					}
					for _, s := range bi.Settings {
						switch s.Key {
						case "vcs.revision":
							if s.Value != "" {
								payload.VCSRevision = s.Value
							}
						case "vcs.time":
							if s.Value != "" {
								payload.VCSTime = s.Value
							}
						case "vcs.modified":
							if b, err := strconv.ParseBool(s.Value); err == nil {
								payload.VCSModified = &b
							}
						}
					}
				}
			}
			enc := json.NewEncoder(out)
			enc.SetEscapeHTML(false)
			return enc.Encode(payload)
		}

		if extended {
			// Extended version output
			_, _ = fmt.Fprintf(out, "%s %s\n", binaryName, ver)
			_, _ = fmt.Fprintf(out, "Commit: %s\n", commit)
			_, _ = fmt.Fprintf(out, "Built: %s\n", buildDate)
			_, _ = fmt.Fprintf(out, "Go: %s\n", runtime.Version())
			_, _ = fmt.Fprintf(out, "\n")

			// Gofulmen and Crucible versions
			version := crucible.GetVersion()
			_, _ = fmt.Fprintf(out, "Gofulmen: %s\n", version.Gofulmen)
			_, _ = fmt.Fprintf(out, "Crucible: %s\n", version.Crucible)
		} else {
			// Basic version output
			_, _ = fmt.Fprintf(out, "%s %s\n", binaryName, ver)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
	versionCmd.Flags().BoolVarP(&extended, "extended", "e", false, "show extended version information")
	versionCmd.Flags().BoolVar(&versionJSON, "json", false, "emit version information as single-line JSON")
}
