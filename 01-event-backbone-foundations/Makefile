setup:
	go install github.com/cespare/reflex@latest

delete-topic:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic marketplace.listings.v1

emitter:
	reflex -s -r '\.go$$' go run cmd/emitter/main.go

indexer:
	reflex -s -r '\.go$$' go run cmd/indexer/main.go

lag:
	go run cmd/lag/main.go

# Generates bulk events
bulk:
	./bulk.sh

# Restart consumer group
cs-reset:
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 \
      --group listing-indexer-v1 \
      --topic marketplace.listings.v1 \
      --reset-offsets --to-earliest \
      --execute

# Reset to a timestamp
cs-reset-timestamp:
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server localhost:9092 \
	  --group listing-indexer-v1 \
	  --topic marketplace.listings.v1 \
	  --reset-offsets --to-datetime 2026-08-07T10:00:00.000 \
	  --execute

# Reset partition
cs-reset-partition:
	docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 \
      --group listing-indexer-v1 \
      --topic marketplace.listings.v1:3 \
      --reset-offsets --to-offset 50  \
	  --execute