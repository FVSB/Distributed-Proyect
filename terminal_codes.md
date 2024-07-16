
# Para iniciar el contenerdor cualquiera
```bash
docker run --rm -it -v /home/Distributed-Proyect
/distributed:/app/code -v /home/Distributed-Proyect/logs/container_${numero del contenedor a levantar}:/app/logs cp4 /bin/bash

```
# Dentro del contenerdor server
- La carpeta app  tiene dos directorios
    - code: Esta todo lo que se desea levantar
    - logs: se almacenan los logs 
Para levantar el codigo desde la terminal del contenedor 
```bash 
python app/code/chord.py ${Direccion ip a la que hacer join si lo ves necesario}
```
