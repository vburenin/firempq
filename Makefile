all: clean build

build: protobuf
	go build firempq.go

install:
	go install firempq

protobuf:
	find ./server -name "*.proto" -type f -print0 | xargs -0 -n 1 protoc --gogoslick_out=.
	find ./pqueue -name "*.proto" -type f -print0 | xargs -0 -n 1 protoc --gogoslick_out=.
	find ./conf -name "*.proto" -type f -print0 | xargs -0 -n 1 protoc --gogoslick_out=.
	find ./queue_info -name "*.proto" -type f -print0 | xargs -0 -n 1 protoc --gogoslick_out=.

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

