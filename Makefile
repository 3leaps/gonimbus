.PHONY: all help bootstrap bootstrap-force hooks-ensure tools sync dependencies verify-dependencies version-bump lint test build build-all clean fmt version check-all precommit prepush run install test-cov
.PHONY: license-inventory license-save license-audit update-licenses
.PHONY: sync-embedded-identity verify-embedded-identity
.PHONY: release-clean release-download release-sign release-export-keys release-verify-keys release-checksums release-verify-checksums release-notes release-upload release-upload-provenance release-upload-all release-guard-tag-version
.PHONY: version-set version-bump-major version-bump-minor version-bump-patch release-check release-prepare release-build

# Binary and version information
BINARY_NAME := gonimbus
VERSION := $(shell cat VERSION 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(BUILD_DATE)

# Go related variables
GOCMD := go
GOTEST := $(GOCMD) test
GOFMT := $(GOCMD) fmt
GOMOD := $(GOCMD) mod

# Tool installation (user-space bin dir; overridable with BINDIR=...)
#
# Defaults:
# - macOS/Linux: $HOME/.local/bin
# - Windows (Git Bash / MSYS / MINGW / Cygwin): %USERPROFILE%\\bin (or $HOME/bin)
BINDIR ?=
BINDIR_RESOLVE = \
	BINDIR="$(BINDIR)"; \
	if [ -z "$$BINDIR" ]; then \
		OS_RAW="$$(uname -s 2>/dev/null || echo unknown)"; \
		case "$$OS_RAW" in \
			MINGW*|MSYS*|CYGWIN*) \
				if [ -n "$$USERPROFILE" ]; then \
					if command -v cygpath >/dev/null 2>&1; then \
						BINDIR="$$(cygpath -u "$$USERPROFILE")/bin"; \
					else \
						BINDIR="$$USERPROFILE/bin"; \
					fi; \
				elif [ -n "$$HOME" ]; then \
					BINDIR="$$HOME/bin"; \
				else \
					BINDIR="./bin"; \
				fi ;; \
			*) \
				if [ -n "$$HOME" ]; then \
					BINDIR="$$HOME/.local/bin"; \
				else \
					BINDIR="./bin"; \
				fi ;; \
		esac; \
	fi

# Tooling
GONEAT_VERSION := v0.3.21

SFETCH_RESOLVE = \
	$(BINDIR_RESOLVE); \
	SFETCH=""; \
	if [ -x "$$BINDIR/sfetch" ]; then SFETCH="$$BINDIR/sfetch"; fi; \
	if [ -z "$$SFETCH" ]; then SFETCH="$$(command -v sfetch 2>/dev/null || true)"; fi

GONEAT_RESOLVE = \
	$(BINDIR_RESOLVE); \
	GONEAT=""; \
	if [ -x "$$BINDIR/goneat" ]; then GONEAT="$$BINDIR/goneat"; fi; \
	if [ -z "$$GONEAT" ]; then GONEAT="$$(command -v goneat 2>/dev/null || true)"; fi; \
	if [ -z "$$GONEAT" ]; then echo "❌ goneat not found. Run 'make bootstrap' first."; exit 1; fi

# Default target
all: fmt test

help:  ## Show this help message
	@printf '%s\n' '$(BINARY_NAME) - Available Make Targets' '' 'Required targets (Makefile Standard):' '  help            - Show this help message' '  bootstrap       - Install external tools (sfetch, goneat) and dependencies' '  bootstrap-force - Force reinstall external tools' '  tools           - Verify external tools are available' '  dependencies    - Generate SBOM for supply-chain security' '  lint            - Run lint/format/style checks' '  test            - Run all tests' '  build           - Build distributable artifacts' '  build-all       - Build multi-platform binaries' '  clean           - Remove build artifacts and caches' '  fmt             - Format code' '  version         - Print current version' '  version-set     - Set version to specific value' '  version-bump-major - Bump major version' '  version-bump-minor - Bump minor version' '  version-bump-patch - Bump patch version' '  release-check   - Run release checklist validation' '  release-prepare - Prepare for release' '  release-build   - Build release artifacts' '  check-all       - Run all quality checks (fmt, lint, test)' '  precommit       - Run pre-commit hooks (check-all)' '  prepush         - Run pre-push hooks (check-all)' '' 'Additional targets:' '  run             - Run server in development mode' '  test-cov        - Run tests with coverage report' ''

bootstrap:  ## Install external tools (sfetch, goneat) and dependencies
	@echo "Installing external tools..."
	@$(SFETCH_RESOLVE); if [ -z "$$SFETCH" ]; then echo "❌ sfetch not found (required trust anchor)."; echo ""; echo "Install sfetch, verify it, then re-run bootstrap:"; echo "  curl -sSfL https://github.com/3leaps/sfetch/releases/latest/download/install-sfetch.sh | bash"; echo "  sfetch --self-verify"; echo ""; exit 1; fi
	@$(BINDIR_RESOLVE); mkdir -p "$$BINDIR"; echo "→ sfetch self-verify (trust anchor):"; $(SFETCH_RESOLVE); $$SFETCH --self-verify
	@$(BINDIR_RESOLVE); if [ "$(FORCE)" = "1" ] || [ "$(FORCE)" = "true" ]; then rm -f "$$BINDIR/goneat" "$$BINDIR/goneat.exe"; fi; echo "→ Installing goneat $(GONEAT_VERSION) to user bin dir..."; $(SFETCH_RESOLVE); $(BINDIR_RESOLVE); $$SFETCH --repo fulmenhq/goneat --tag $(GONEAT_VERSION) --dest-dir "$$BINDIR"; OS_RAW="$$(uname -s 2>/dev/null || echo unknown)"; case "$$OS_RAW" in MINGW*|MSYS*|CYGWIN*) if [ -f "$$BINDIR/goneat.exe" ] && [ ! -f "$$BINDIR/goneat" ]; then mv "$$BINDIR/goneat.exe" "$$BINDIR/goneat"; fi ;; esac; $(GONEAT_RESOLVE); echo "→ goneat: $$($$GONEAT --version 2>&1 | head -n1 || true)"; echo "→ Installing foundation tools via goneat doctor..."; $$GONEAT doctor tools --scope foundation --install --install-package-managers --yes --no-cooling
	@echo "→ Downloading Go module dependencies..."; go mod download; go mod tidy; $(MAKE) hooks-ensure; $(BINDIR_RESOLVE); echo "✅ Bootstrap completed. Ensure $$BINDIR is on PATH"

bootstrap-force:  ## Force reinstall external tools
	@$(MAKE) bootstrap FORCE=1

hooks-ensure:  ## Ensure git hooks are installed (idempotent)
	@$(BINDIR_RESOLVE); \
	GONEAT=""; \
	if [ -x "$$BINDIR/goneat" ]; then GONEAT="$$BINDIR/goneat"; fi; \
	if [ -z "$$GONEAT" ]; then GONEAT="$$(command -v goneat 2>/dev/null || true)"; fi; \
	if [ -d ".git" ] && [ -n "$$GONEAT" ] && [ ! -x ".git/hooks/pre-commit" ]; then \
		echo "🔗 Installing git hooks with goneat..."; \
		$$GONEAT hooks install 2>/dev/null || true; \
	fi

tools:  ## Verify external tools are available
	@echo "Verifying external tools..."
	@$(GONEAT_RESOLVE); echo "✅ goneat: $$($$GONEAT --version 2>&1 | head -n1)"
	@echo "✅ All tools verified"

sync:  ## Sync assets from Crucible SSOT (placeholder)
	@echo "⚠️  Gonimbus does not consume SSOT assets directly"
	@echo "✅ Sync target satisfied (no-op)"

dependencies:  ## Generate SBOM for supply-chain security
	@echo "Generating Software Bill of Materials (SBOM)..."; $(GONEAT_RESOLVE); $$GONEAT dependencies --sbom --sbom-output sbom/$(BINARY_NAME).cdx.json
	@echo "✅ SBOM generated at sbom/$(BINARY_NAME).cdx.json"

verify-dependencies:  ## Alias for dependencies (compatibility)
	@$(MAKE) dependencies

install: build ## Install binary to user bin directory
	@$(BINDIR_RESOLVE); \
	mkdir -p "$$BINDIR"; \
	cp bin/$(BINARY_NAME) "$$BINDIR/$(BINARY_NAME)"; \
	chmod 755 "$$BINDIR/$(BINARY_NAME)"; \
	echo "✅ Installed $(BINARY_NAME) to $$BINDIR/$(BINARY_NAME)"

run:  ## Run server in development mode
	@go run ./cmd/$(BINARY_NAME) serve --verbose

version-bump:  ## Bump version (usage: make version-bump TYPE=patch|minor|major|calver)
	@if [ -z "$(TYPE)" ]; then \
		echo "❌ TYPE not specified. Usage: make version-bump TYPE=patch|minor|major|calver"; \
		exit 1; \
	fi
	@echo "Bumping version ($(TYPE))..."; $(GONEAT_RESOLVE); $$GONEAT version bump $(TYPE)
	@echo "✅ Version bumped to $$(cat VERSION)"

version-set:  ## Set version to specific value (usage: make version-set VERSION=x.y.z)
	@if [ -z "$(VERSION)" ]; then \
		echo "❌ VERSION not specified. Usage: make version-set VERSION=x.y.z"; \
		exit 1; \
	fi
	@echo "$(VERSION)" > VERSION
	@echo "✅ Version set to $(VERSION)"

version-bump-major:  ## Bump major version
	@$(MAKE) version-bump TYPE=major

version-bump-minor:  ## Bump minor version
	@$(MAKE) version-bump TYPE=minor

version-bump-patch:  ## Bump patch version
	@$(MAKE) version-bump TYPE=patch

release-check:  ## Run release checklist validation
	@echo "Running release checklist..."
	@$(MAKE) check-all
	@echo "✅ Release check passed"

release-prepare:  ## Prepare for release (tests, version bump)
	@echo "Preparing release..."
	@$(MAKE) check-all
	@echo "✅ Release preparation complete"

# ─────────────────────────────────────────────────────────────────────────────
# Manual signing workflow helpers (minisign primary + optional PGP)
# - Stages artifacts in dist/release to avoid bin/ footguns
# - Generates SHA256SUMS and SHA512SUMS manifests
# - Signs manifests only (do not sign each artifact)
# - Env vars (all GONIMBUS_ prefixed):
#     GONIMBUS_MINISIGN_KEY - path to minisign secret key
#     GONIMBUS_MINISIGN_PUB - path to minisign public key (optional)
#     GONIMBUS_PGP_KEY_ID   - gpg key for PGP signing (optional)
#     GONIMBUS_GPG_HOMEDIR  - isolated gpg homedir (required if PGP_KEY_ID set)
# ─────────────────────────────────────────────────────────────────────────────

RELEASE_TAG ?= v$(shell cat VERSION 2>/dev/null || echo "0.0.0")
DIST_RELEASE ?= dist/release

sync-embedded-identity: ## Sync embedded identity mirror from .fulmen/app.yaml
	@./scripts/sync-embedded-identity.sh

verify-embedded-identity: ## Verify embedded identity mirror is in sync
	@./scripts/verify-embedded-identity.sh

release-clean: ## Clean dist/release staging
	@echo "🧹 Cleaning $(DIST_RELEASE)..."; rm -rf "$(DIST_RELEASE)"; mkdir -p "$(DIST_RELEASE)"; echo "✅ Cleaned"

release-build: sync-embedded-identity release-clean ## Build release artifacts into dist/release
	@echo "→ Building release artifacts for $(BINARY_NAME) v$(VERSION)..."
	@mkdir -p "$(DIST_RELEASE)"
	@GOOS=linux GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o "$(DIST_RELEASE)/$(BINARY_NAME)-linux-amd64" ./cmd/$(BINARY_NAME)
	@GOOS=darwin GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o "$(DIST_RELEASE)/$(BINARY_NAME)-darwin-amd64" ./cmd/$(BINARY_NAME)
	@GOOS=darwin GOARCH=arm64 go build -ldflags="$(LDFLAGS)" -o "$(DIST_RELEASE)/$(BINARY_NAME)-darwin-arm64" ./cmd/$(BINARY_NAME)
	@GOOS=windows GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o "$(DIST_RELEASE)/$(BINARY_NAME)-windows-amd64.exe" ./cmd/$(BINARY_NAME)
	@GOOS=linux GOARCH=arm64 go build -ldflags="$(LDFLAGS)" -o "$(DIST_RELEASE)/$(BINARY_NAME)-linux-arm64" ./cmd/$(BINARY_NAME)
	@$(MAKE) release-checksums
	@echo "✅ Release build complete"

# release-checksums generates checksum manifests inside dist/release.
release-checksums: ## Generate SHA256SUMS and SHA512SUMS in dist/release
	@echo "→ Generating checksum manifests in $(DIST_RELEASE)..."
	@./scripts/generate-checksums.sh "$(DIST_RELEASE)" "$(BINARY_NAME)"

# Deprecated alias (kept for one cycle).
checksums: release-checksums ## Deprecated: use release-checksums
	@:

release-guard-tag-version: ## Guard: ensure RELEASE_TAG matches VERSION file
	@TAG="$(RELEASE_TAG)"; \
	EXPECTED="v$(VERSION)"; \
	if [ "$$TAG" != "$$EXPECTED" ]; then \
		echo "❌ RELEASE_TAG mismatch: $$TAG != $$EXPECTED (from VERSION)" >&2; \
		exit 1; \
	fi; \
	echo "✅ RELEASE_TAG matches VERSION ($$TAG)"

release-download: release-guard-tag-version ## Download GitHub release assets (RELEASE_TAG=vX.Y.Z)
	@./scripts/release-download.sh "$(RELEASE_TAG)" "$(DIST_RELEASE)"

release-sign: ## Sign checksum manifests (minisign required; PGP optional)
	@./scripts/sign-release-manifests.sh "$(RELEASE_TAG)" "$(DIST_RELEASE)"

release-export-keys: ## Export public signing keys into dist/release
	@./scripts/export-release-keys.sh "$(DIST_RELEASE)"

release-verify-keys: ## Verify exported public keys are public-only
	@if [ -f "$(DIST_RELEASE)/gonimbus-minisign.pub" ]; then ./scripts/verify-minisign-public-key.sh "$(DIST_RELEASE)/gonimbus-minisign.pub"; else echo "ℹ️  No minisign public key found (skipping)"; fi
	@if [ -f "$(DIST_RELEASE)/gonimbus-release-signing-key.asc" ]; then ./scripts/verify-public-key.sh "$(DIST_RELEASE)/gonimbus-release-signing-key.asc"; else echo "ℹ️  No PGP public key found (skipping)"; fi

# Deprecated alias (kept for one cycle).
verify-release-keys: release-verify-keys ## Deprecated: use release-verify-keys
	@:

release-notes: ## Copy docs/releases/vX.Y.Z.md into dist/release
	@notes_src="docs/releases/$(RELEASE_TAG).md"; notes_dst="$(DIST_RELEASE)/release-notes-$(RELEASE_TAG).md"; \
	if [ ! -f "$$notes_src" ]; then echo "❌ Missing $$notes_src"; exit 1; fi; \
	cp "$$notes_src" "$$notes_dst"; echo "✅ Copied $$notes_src → $$notes_dst"

release-verify-checksums: ## Verify SHA256SUMS and SHA512SUMS against artifacts
	@./scripts/verify-checksums.sh "$(DIST_RELEASE)"

# Deprecated alias (kept for one cycle).
verify-checksums: release-verify-checksums ## Deprecated: use release-verify-checksums
	@:

release-upload: release-upload-provenance ## Upload provenance assets to GitHub (RELEASE_TAG=vX.Y.Z)
	@:

release-upload-provenance: release-verify-checksums release-verify-keys ## Upload manifests, signatures, keys, notes
	@./scripts/release-upload-provenance.sh "$(RELEASE_TAG)" "$(DIST_RELEASE)"

release-upload-all: release-verify-checksums release-verify-keys ## Upload binaries + provenance (manual-only)
	@./scripts/release-upload.sh "$(RELEASE_TAG)" "$(DIST_RELEASE)"

build: sync-embedded-identity ## Build binary for current platform
	@echo "→ Building $(BINARY_NAME) v$(VERSION)..."
	@go build -ldflags="$(LDFLAGS)" -o bin/$(BINARY_NAME) ./cmd/$(BINARY_NAME)
	@echo "✓ Binary built: bin/$(BINARY_NAME)"

build-all:  ## Build multi-platform binaries and generate checksums (dev convenience; prefer release-build for releases)
	@echo "→ Building for multiple platforms..."
	@mkdir -p bin
	@GOOS=linux GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o bin/$(BINARY_NAME)-linux-amd64 ./cmd/$(BINARY_NAME)
	@GOOS=darwin GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o bin/$(BINARY_NAME)-darwin-amd64 ./cmd/$(BINARY_NAME)
	@GOOS=darwin GOARCH=arm64 go build -ldflags="$(LDFLAGS)" -o bin/$(BINARY_NAME)-darwin-arm64 ./cmd/$(BINARY_NAME)
	@GOOS=windows GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o bin/$(BINARY_NAME)-windows-amd64.exe ./cmd/$(BINARY_NAME)
	@GOOS=linux GOARCH=arm64 go build -ldflags="$(LDFLAGS)" -o bin/$(BINARY_NAME)-linux-arm64 ./cmd/$(BINARY_NAME)
	@cd bin && (sha256sum * > SHA256SUMS.txt 2>/dev/null || shasum -a 256 * > SHA256SUMS.txt)
	@echo "✓ Multi-platform binaries built in bin/"

version:  ## Print current version
	@echo "$(VERSION)"

test: sync-embedded-identity ## Run all tests
	@echo "Running test suite..."
	$(GOTEST) ./... -v

test-cov:  ## Run tests with coverage
	@echo "Running tests with coverage..."
	$(GOTEST) ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "✓ Coverage report: coverage.html"

lint:  ## Run lint checks
	@echo "Running goneat assess (lint)..."; $(GONEAT_RESOLVE); $$GONEAT assess --categories lint
	@echo "✅ Lint checks passed"

fmt:  ## Format code with goneat
	@echo "Formatting with goneat..."; $(GONEAT_RESOLVE); $$GONEAT format
	@$(MAKE) sync-embedded-identity
	@echo "✅ Formatting completed"

check-all: fmt verify-embedded-identity lint test  ## Run all quality checks (ensures fmt, lint, test)
	@echo "✅ All quality checks passed"

precommit:  ## Run pre-commit hooks
	@echo "Running pre-commit validation..."; $(GONEAT_RESOLVE); $$GONEAT format; $$GONEAT assess --check --categories format,lint --fail-on critical
	@echo "✅ Pre-commit checks passed"

prepush: license-audit ## Run pre-push hooks
	@echo "Running pre-push validation..."; $(GONEAT_RESOLVE); $$GONEAT format; $$GONEAT assess --check --categories format,lint,security --fail-on high
	@echo "✅ Pre-push checks passed"

# License compliance
license-inventory: ## Generate CSV inventory of dependency licenses
	@echo "Generating license inventory (CSV)..."
	@mkdir -p docs/licenses dist/reports
	@if ! command -v go-licenses >/dev/null 2>&1; then \
		echo "Installing go-licenses..."; \
		go install github.com/google/go-licenses@latest; \
	fi
	go-licenses csv ./... > docs/licenses/inventory.csv
	@echo "✅ Wrote docs/licenses/inventory.csv"

license-save: ## Save third-party license texts
	@echo "Saving third-party license texts..."
	@rm -rf docs/licenses/third-party
	@if ! command -v go-licenses >/dev/null 2>&1; then \
		echo "Installing go-licenses..."; \
		go install github.com/google/go-licenses@latest; \
	fi
	go-licenses save ./... --save_path=docs/licenses/third-party
	@echo "✅ Saved third-party licenses to docs/licenses/third-party"

license-audit: ## Audit for forbidden licenses and cooling policy
	@echo "Auditing dependency licenses and cooling policy..."; $(GONEAT_RESOLVE); $$GONEAT assess --categories dependencies --check --fail-on high
	@echo "✅ License audit passed"

update-licenses: license-inventory license-save ## Update license inventory and texts

clean:  ## Clean build artifacts and reports
	@echo "Cleaning artifacts..."
	rm -rf bin/ dist/ coverage.out coverage.html
	@echo "✅ Clean completed"
