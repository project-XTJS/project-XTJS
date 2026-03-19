SHELL := /bin/sh
.DEFAULT_GOAL := run

version ?= release
commit_id ?= $(shell git rev-parse --short HEAD)
project ?= $(CURDIR)
project_name ?= project-xtjs
image ?= $(project_name):$(version)-$(commit_id)

compose_file ?= docker-compose.yml
python_bin ?= python

.PHONY: all build fmt clean test lint help run deploy start package prepare update_docker_compose status stop

help:
	@echo "Usage: make <target> [version=<v>] [commit_id=<id>] [project_name=<name>]"
	@echo ""
	@echo "Targets:"
	@echo "  all                    Alias of run"
	@echo "  deploy                 Alias of run"
	@echo "  run                    update_docker_compose -> package -> prepare -> start"
	@echo "  update_docker_compose  Replace $(project_name):* to $(image) in $(compose_file)"
	@echo "  package                docker build --provenance=false -t $(image) ."
	@echo "  prepare                docker compose down"
	@echo "  start                  docker compose up -d"
	@echo "  status                 docker compose ps"
	@echo "  stop                   docker compose down"

all: run

deploy: run

run: update_docker_compose package prepare start

start:
	@if [ ! -f "$(compose_file)" ]; then \
		echo "[skip] $(compose_file) not found."; \
		exit 0; \
	fi
	docker compose up -d

package:
	@if [ ! -f Dockerfile ]; then \
		echo "[skip] Dockerfile not found."; \
		exit 0; \
	fi
	docker build --provenance=false -t $(image) .

prepare:
	@if [ ! -f "$(compose_file)" ]; then \
		echo "[skip] $(compose_file) not found."; \
		exit 0; \
	fi
	docker compose down

update_docker_compose:
	@if [ ! -f "$(compose_file)" ]; then \
		echo "[skip] $(compose_file) not found."; \
		exit 0; \
	fi
	@if command -v sed >/dev/null 2>&1; then \
		sed -i "s#$(project_name):[^[:space:]]*#$(image)#g" "$(project)/$(compose_file)"; \
		echo "updated $(project)/$(compose_file) by sed"; \
	else \
		PROJECT_FILE="$(project)/$(compose_file)" PROJECT_NAME="$(project_name)" IMAGE="$(image)" "$(python_bin)" -c "import os,pathlib,re; p=pathlib.Path(os.environ['PROJECT_FILE']); s=p.read_text(encoding='utf-8'); pat=re.escape(os.environ['PROJECT_NAME']) + r':[^\s\"\x27]+'; n,c=re.subn(pat, os.environ['IMAGE'], s); p.write_text(n,encoding='utf-8'); print('updated', p, 'replacements=', c)"; \
	fi

status:
	@if [ ! -f "$(compose_file)" ]; then \
		echo "[skip] $(compose_file) not found."; \
		exit 0; \
	fi
	docker compose ps

stop:
	@if [ ! -f "$(compose_file)" ]; then \
		echo "[skip] $(compose_file) not found."; \
		exit 0; \
	fi
	docker compose down
