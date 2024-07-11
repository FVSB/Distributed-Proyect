import threading
import time

def tarea_en_fondo():
    print("Hilo demonio iniciado")
    time.sleep(6)  # Este hilo dormiría durante 10 segundos
    print("Hilo demonio despertando")

# Crear un hilo demonio
hilo_demonio = threading.Thread(target=tarea_en_fondo)
hilo_demonio.daemon = True  # Marcar el hilo como demonio
hilo_demonio.start()

# Ejecutar algo en el hilo principal
print("Ejecutando algo en el hilo principal")
time.sleep(5)  # Hacer que el hilo principal duerma durante 5 segundos
print("Programa principal terminando")
