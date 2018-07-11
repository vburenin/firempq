all: clean build

build: protobuf
	go build cmd/firempq/firempq.go

install:
	go install cmd/firempq/firempq.go

protobuf:
	find ./server -name "*.proto" -type f -print0 | xargs -0 -n 1 protoc --gogoslick_out=.
	find ./qconf -name "*.proto" -type f -print0 | xargs -0 -n 1 protoc --gogoslick_out=.
	find ./pmsg -name "*.proto" -type f -print0 | xargs -0 -n 1 protoc --gogoslick_out=.

clean:
	go clean ./...

test: protobuf
	go test ./pqueue/...
	go test ./mpqproto/...

vet:
	go vet ./...
	go tool vet --shadow .

run: build
	./firempq

