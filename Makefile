start-docker:
	cd ./ksql-docker && docker-compose up -d

stop-docker:
	cd ./ksql-docker && docker-compose down

restart-docker: stop-docker start-docker

start-consumer:
	cd ./ksql-service/consumer && go run main.go

start-producer:
	cd ./ksql-service/producer && go run main.go