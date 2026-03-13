version?=release
commit_id?=$(shell git rev-parse --short HEAD)
project?=$(CURDIR)
project_name?=project-xtjs
image?=project-xtjs:${version}-${commit_id}
paddle_base_image?=nvidia/cuda:13.0.0-cudnn-devel-ubuntu24.04
paddle_version?=3.3.0
paddle_index_url?=https://www.paddlepaddle.org.cn/packages/stable/cu130/
compose_files?=-f docker-compose.yml -f docker-compose.gpu.yml
compose?=docker compose ${compose_files}
python_cmd?=python3

run: update_docker_compose package prepare start

start:
	${compose} up -d

package:
	docker pull ${paddle_base_image}
	docker build \
		--build-arg PADDLE_BASE_IMAGE=${paddle_base_image} \
		--build-arg PADDLE_VERSION=${paddle_version} \
		--build-arg PADDLE_INDEX_URL=${paddle_index_url} \
		-t ${image} .

prepare:
	${compose} down

update_docker_compose:
	@${python_cmd} -c "from pathlib import Path; p = Path('${project}/docker-compose.yml'); lines = p.read_text(encoding='utf-8').splitlines(); updated = [('    image: ${image}' if line.strip().startswith('image: ${project_name}:') else line) for line in lines]; p.write_text('\n'.join(updated) + '\n', encoding='utf-8')"

status:
	${compose} ps

stop:
	${compose} down

.PHONY: all build fmt clean test lint run start package prepare update_docker_compose status stop
