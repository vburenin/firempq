all: clean build

build:
	find . -name "*.proto" -type f -print0 | xargs -0 -n 1 protoc --gogoslick_out=.
	go build firempq

install:
	go install firempq

proto:
	build proto

clean:
	go clean ./...

test:
	go test ./...

vet:
	go vet ./...
	go tool vet --shadow .
