*** CLI NOTES [DOCKER] ***

1. BRINGS CLUSTER DOWN COMPLETELEY 
docker compose down -v


2. STARTS NEW CLUSTER
docker compose up -d


3. VIEWS CLUSTERS
docker ps

4. REMOVE A BROKER FROM CONTAINER
docker rm -f [BROKER_NAME]



*** CLI NOTES [KAFKA] ***

1. CREATE TOPIC MANUALLY 
docker exec broker1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 6 \
  --replication-factor 3

  EX. --partitions 6: Basically tells Kafka split this topic into six pertiitions
      --replication-factor 3: Bascially stores copies of every partition

      "Go inside the broker1 container, connect to the Kafka cluster through Broker 1, and create a topic named orders that has 6 partitions, with each partition replicated across all 3 brokers."





