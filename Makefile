.PHONY: help all check fmt vet build test release

CYAN   := \033[1;36m
YELLOW := \033[1;33m
RESET  := \033[0m

all: check

help:
	@printf "$(CYAN)Targets$(RESET)\n"
	@printf "  check                      Everything CI runs: fmt, vet, build, test\n"
	@printf "  test                       Run the tests with the race detector\n"
	@printf "  release VERSION=1.0.0      Tag and publish a release\n"
	@printf "  release VERSION=1.0.0 NOTES=1\n"
	@printf "                             Same, but write the release notes by hand\n"

check: fmt vet build test

fmt:
	@printf "\n$(CYAN)Checking formatting...$(RESET)\n"
	@test -z "$$(gofmt -l .)" || { gofmt -d .; exit 1; }

vet:
	@printf "\n$(CYAN)Vetting...$(RESET)\n"
	@go vet ./...

build:
	@printf "\n$(CYAN)Building...$(RESET)\n"
	@go build ./...

test:
	@printf "\n$(CYAN)Running tests...$(RESET)\n"
	@go test -race ./...

release:
	@if [ -z "$(VERSION)" ]; then \
		printf "You must specify a version, e.g. \`make release VERSION=1.0.0\` (pass NOTES=1 to write release notes manually)\n"; \
		exit 1; \
	fi
	@case "$(VERSION)" in v*) \
		printf "Leave the leading v off the version, e.g. \`make release VERSION=%s\`\n" "$$(printf '%s' '$(VERSION)' | cut -c2-)"; \
		exit 1 ;; \
	esac
	@branch=$$(git rev-parse --abbrev-ref HEAD); \
	if [ "$$branch" != "main" ]; then \
		printf "$(YELLOW)Warning: You are on branch '%s', not 'main'. Continue? [y/N] $(RESET)" "$$branch"; \
		read -r reply; \
		case "$$reply" in [yY]*) ;; *) printf "Release aborted.\n"; exit 1 ;; esac; \
	fi
	@if [ -n "$$(git status --porcelain)" ]; then \
		printf "$(YELLOW)Warning: You have uncommitted changes. Continue? [y/N] $(RESET)"; \
		read -r reply; \
		case "$$reply" in [yY]*) ;; *) printf "Release aborted.\n"; exit 1 ;; esac; \
	fi
	@$(MAKE) --no-print-directory check
	@printf "\n$(CYAN)Creating GitHub release v$(VERSION)...$(RESET)\n"
	@if [ -n "$(NOTES)" ]; then \
		notes=$$(mktemp); \
		printf "# What's Changed\n\n- \n" > $$notes; \
		$${EDITOR:-vim} $$notes; \
		gh release create "v$(VERSION)" --title "v$(VERSION)" --notes-file "$$notes" || \
			{ rm -f "$$notes"; printf "Failed to create release; check that the version doesn't already exist\n"; exit 1; }; \
		rm -f "$$notes"; \
	else \
		gh release create "v$(VERSION)" --title "v$(VERSION)" --generate-notes || \
			{ printf "Failed to create release; check that the version doesn't already exist\n"; exit 1; }; \
	fi
	@printf "\n$(CYAN)Warming the module proxy...$(RESET)\n"
	@module=$$(go list -m); \
	GOPROXY=proxy.golang.org go list -m "$$module@v$(VERSION)" >/dev/null 2>&1 || true; \
	printf "\nReleased v$(VERSION).\n  go get $$module@v$(VERSION)\n"
