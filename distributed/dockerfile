# Usamos la imagen local cp4 como base
FROM python:3.11-buster

# Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /

# Copiamos el archivo requirements.txt (si lo tienes) al contenedor
COPY requirements.txt requirements.txt

# Instalamos las dependencias listadas en requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Instalar el modelo para separar las oraciones:
RUN python -m spacy download en_core_web_sm
# instalar dependencias
RUN pip install pyro5 rabbitpy pyzmq rpyc
# Instalar streamlit
RUN pip install streamlit
# Copiamos el resto de la aplicación al contenedor
#COPY . .

# Exponemos el puerto en el que correrá la aplicación Flask
EXPOSE 8001

# Comando para ejecutar la aplicación
CMD ["python", "app.py"]
