Para correr el proyecto

- Construir la imagen de docker 
- Levantar el docker compose
  - ``` docker-compose up -d ```
  
- levantar los nodos
    - ```docker exec -it  node_{$i}  bin
/bash
```
    - ```python app/code/server.py```
    - levantar cliente ```docker exec -it  client_1  b
in/bash```
- Se recomienda 3  o mas para garantizar resistencia >=2
- en el cliente ```streamlit run app/code/index.py```
- en los contenedores server ```python app/code/server.py```
- Las base de datos se linkean directamente en cada contenedor
- Los logs analogamente
- Recordar que en el server en la parte de if __name__ se puede cambiar las configuraciones
- Tener activado el server de embeddings de lmstudio
- Si se cambia de generador de embeddings
- ir a distributed/helper/embedding_generator.py y mirar la api client = OpenAI(base_url="http://host.docker.internal:1234/v1", api_key="lm-studio") o la que se vaya a usar