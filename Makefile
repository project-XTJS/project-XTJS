version?=release
commit_id?=$(shell git rev-parse --short HEAD)
project?=$(CURDIR)
project_name?=project-xtjs
image?=project-xtjs:${version}-${commit_id}
compose_files?=-f docker-compose.yml -f docker-compose.gpu.yml
compose?=docker compose ${compose_files}
python_cmd?=python3

run: update_docker_compose package prepare start

start:
	${compose} up -d

package:
	docker build -t ${image} .

prepare:
	${compose} down

update_docker_compose:
	@${python_cmd} -c "from pathlib import Path; p = Path('${project}/docker-compose.yml'); lines = p.read_text(encoding='utf-8').splitlines(); updated = [('    image: ${image}' if line.strip().startswith('image: ${project_name}:') else line) for line in lines]; p.write_text('\n'.join(updated) + '\n', encoding='utf-8')"

status:
	${compose} ps

stop:
	${compose} down

.PHONY: all build fmt clean test lint run start package prepare update_docker_compose status stop
