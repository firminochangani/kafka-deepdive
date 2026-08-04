setup:
	go install github.com/cespare/reflex@latest

delete-topic:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic marketplace.listings.v1

emitter:
	reflex -s -r '\.go$$' go run cmd/emitter/main.go

indexer:
	reflex -s -r '\.go$$' go run cmd/indexer/main.go