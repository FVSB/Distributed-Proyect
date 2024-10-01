#!/bin/bash

# Levantar Docker Compose
docker-compose up -d

# Esperar un momento para asegurarse de que los contenedores estén en funcionamiento
sleep 5

# Levantar 5 terminales con el comando docker exec
for i in {1..5}
do
    gnome-terminal --title "Terminal para node_$i" -- bash -c "docker exec -it node_$i bin/bash"
done


gnome-terminal --title "Terminal para Cliente_1" -- bash -c "docker exec -it client_1 bin/bash"