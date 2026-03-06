version?=release
commit_id?=fd92b4a
project?=$(shell pwd)
project_name?=project-xtjs
image?=project-xtjs:${version}-${commit_id}

run: update_docker_compose package prepare start

start:
	docker compose up -d

package:
	docker build -t ${image} .

prepare:
	docker compose down

update_docker_compose:
	@sed -i 's/project-xtjs:.*/${image}/g' ${project}/docker-compose.yml

status:
	docker compose ps

stop:
	docker compose down

.PHONY: all build fmt clean test lint run start package prepare update_docker_compose status stop