all: clean build

build: protobuf
	go build firempq

install:
	go install firempq

protobuf:
	find . -name "*.proto" -type f -print0 | xargs -0 -n 1 protoc --gogoslick_out=.

clean:
	go clean ./...

test: protobuf
	go test ./...

vet:
	go vet ./...
	go tool vet --shadow .

run: build
	./firempq

