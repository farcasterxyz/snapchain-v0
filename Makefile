# Configuration variables
DOCKER_COMPOSE = docker compose
CARGO = cargo
RUSTFLAGS = -Dwarnings
MALACHITE_REPO = git@github.com:informalsystems/malachite.git
MALACHITE_COMMIT = 8a9f3702eb41199bc8a7f45139adba233a04744a
MALACHITE_DIR = ../malachite

# Help target - lists all available targets with descriptions
.PHONY: help
help:
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Development Workflow:"
	@echo "  dev          - Start development environment with watch mode"
	@echo "  local-build  - Build project locally including Malachite dependency"
	@echo ""
	@echo "Code Quality:"
	@echo "  check-commit - Run all code quality checks before committing"
	@echo "  lint         - Run formatting and clippy checks"
	@echo "  fmt          - Format code using rustfmt"
	@echo "  test         - Run test suite"
	@echo ""
	@echo "Docker Commands:"
	@echo "  build        - Build Docker containers"
	@echo "  clean        - Remove all containers, volumes, images and build artifacts"
	@echo ""
	@echo "Examples:"
	@echo "  make dev               - Start development environment"
	@echo "  make check-commit      - Run all checks before committing"
	@echo "  make fmt && make test  - Format code and run tests"
	@echo ""
	@echo "Common Workflows:"
	@echo "  1. First time setup:        make local-build"
	@echo "  2. Regular development:      make dev"
	@echo "  3. Before committing:        make check-commit"
	@echo "  4. Clean environment:        make clean"
	@echo ""

# Set RUSTFLAGS for all cargo commands
export RUSTFLAGS

# Docker-related targets
.PHONY: build
build:
	$(DOCKER_COMPOSE) build --ssh default

.PHONY: dev
dev: build
	$(DOCKER_COMPOSE) up --watch

.PHONY: clean
clean:
	$(DOCKER_COMPOSE) down --remove-orphans --volumes --rmi=all
	$(CARGO) clean

# Local development targets
.PHONY: local-build
local-build: setup-malachite
	$(CARGO) build

.PHONY: setup-malachite
setup-malachite:
	@if [ ! -d "$(MALACHITE_DIR)" ]; then \
		cd .. && git clone $(MALACHITE_REPO); \
	fi
	cd $(MALACHITE_DIR) && \
		git fetch && \
		git checkout $(MALACHITE_COMMIT) && \
		cd code && $(CARGO) build

# Code quality targets
.PHONY: check-commit
check-commit: test lint

.PHONY: test
test:
	$(CARGO) test

.PHONY: lint
lint: fmt-check
	$(CARGO) clippy -- -D warnings

.PHONY: fmt-check
fmt-check:
	$(CARGO) fmt --all --check

.PHONY: fmt
fmt:
	$(CARGO) fmt --all

# Default target
.DEFAULT_GOAL := help

.PHONY: all build dev clean local-build setup-malachite check-commit test lint fmt fmt-check help