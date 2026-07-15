*** CLI NOTES ***

1. TO VIEW THE LIST OF ALL THE TOPICS THAT KAFKA IS KEEPING TRACK OF UTILZE THIS CLI.
"docker exec -it kafka kafka-topics --list --{bootstrap-server localhost:9092}"


2. LISTS THE CONFIGURATION PROPERTIES AND VALUES FOR THE TOPIC.
"docker exec -it kafka kafka-topics --{bootstrap-server localhost:9092} --descirbe --topic orders"





3. 
VIEW LIST OF ORDERS VIA KAFKA CLI
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning


docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --partition 0 \
  --offset 0 \
  --timeout-ms 5000