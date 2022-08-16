infra:
	docker compose up -d

topics:
	docker-compose exec broker kafka-topics --create --bootstrap-server broker:9092 --replication-factor 1 --partitions 1 --topic testtimestamp
	docker-compose exec broker kafka-topics --create --bootstrap-server broker:9092 --replication-factor 1 --partitions 1 --topic testlocaldate
	docker-compose exec broker kafka-topics --create --bootstrap-server broker:9092 --replication-factor 1 --partitions 1 --topic heartbeat

delete-topics:
	docker-compose exec broker kafka-topics --delete --bootstrap-server broker:9092 --topic testtimestamp
	docker-compose exec broker kafka-topics --delete --bootstrap-server broker:9092 --topic testlocaldate
	docker-compose exec broker kafka-topics --delete --bootstrap-server broker:9092 --topic heartbeat