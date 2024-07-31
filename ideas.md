# Server:
- Cada nodo de la red Chord es un servidor SRI Completo.
- Se hace la elección de lider por bagging o proof of work bagging preferentemente para que el nodo con menor id sea el lider con el fin de evitar los problemas de reconexión,
- Todo nodo de la red puede recibir un mensaje del cliente pero debe redireccionar el procesamiento al lider
- El lider es el único que puede mandar a insertar data original
- La replicación está en sus k predecesores, K siendo el valor de tolerancia a fallas
- La data replicada se dice que nodo esta replicando
- Un nodo no puedo hacer ninguna operacion de escritura sobre su data hasta que todos los nodos replicados no lo hacen y esperan confirmación de este, las transacciones IO son atomicas osea se realizan tanto en la parte original como en la replicada simultaneamente
- La data ser guarda como un map(key->tupple(data,historial)) 
    
    - El historial guarda todos los cambios de la data con la hora de la computadora (En principio para conocer eso decimos que tenemos un servicio que lo provee, En caso contrario tener vectores de data).
    - Cuando se inserta por primera vez se crea envia la data y el historial vacio y el nodo encargado de insertarlo como suya debe poner que es una insercción.
    - Cuando se elimina un archivo jamas se elimina su historial.

En caso de Entrada,Salida (Tb Cubre que se parta la red y resincronize):
    - Se crea cada nodo toma su data propia y replicada y lo pasa a un modo "seguro" lo cual no es mas que enviar al lider la petición de reinsertar la llave cuando el lider confirma la inserpcion este la elimina del modo seguro.
    - Se tiene que tener una pequeña cache para los nodos datos que estan siendo reinsertados.
    - En caso de datos duplicados se queda con el mas reciente
        - En caso de haber eliminado el archivo, se toma que no haya habido reincepcion despues y se toma eso o se elimina esto es configurable


- Durante una operacion IO en un nodo esta bloquea su data original, o al menos dicha llave. Uso de concurrencia.
- Si en algún momento de la resincornizacion ocurre una inconsistencia en la red cuando se vuelva a hacer consistence repetir lo mismo desde  el principio añadiendo al modo seguro la data que fue reinsertada.

- Establecer reloj de lamport para tener un cierto orden de las acciones.
- Cuando se realize una query cualquier nodo de la red lo envia al lider el cual recibe la ip y query del cliente que se realiza y envia a toda la red una solicitud que cada una resuelva con sus propios archivos, de esto el lider calculara los mas relevantes, se puede configurar que en caso de morir el lider, los nodos en cuanto reconozcan al nuevo lider procesencla query, tratando la data que tienen mas la 'segura' para realizar el retrival, caso contrario que esperen a estar en modo estable y en ese momento procesen la cola de solicitudes pendientes.

- Todo el tiempo debe estar descubriendose nuevos nodos, asi como actualizando con el lider la cache de ips la cual se intercambia con el cliente en cada conexion, el cliente tb pasa la suya como cookie, para poder tener mas facilidad de descubrimiento.




### Notas:
- Siempre se esta verificando la existencia del predecesor en caso que no exista en unos 10 segundos, lo elimino como predecesor y busco el que me toda
   
    - Si es el caso que mi predecesor es mi sucesor osea que solo hay dos nodos pues eliminar mi sucesor tb
     ```mermaid
        graph TD;
         A-->B;
         B-->A;

    ```




## TODO:
-X Se tiene que en el momento de mandar a salvar en las replicas asegurarse que es estable la lista de sucesores está quitada por temas de comodidad de tiempo.

- Se tiene que añadir la capa de estabilidad con respecto al lider,
   X - En chordLider cuando se termine una eleccion:
        - El lider toma la iniciativa y dice que el ya está estable:
            Todos los nodos si no estan en eleccion y su predecesor esta estable:
            se ponen en estable_  y para el estable preguntan frecuentemente al lider
            si en algun momento esoty en eleccion o mi predecesor lo esta o el lider lo deja de estar
            yo no soy estable
            - El lider tiene una variable bool Networt Stable cuando ve que su predecesor esta estable
        - Con respecto a succ_ok se revisa que el lider este ok primero y despues que cada uno de esos nodos esta ok
    - En la sincronización cuando se establece todos los nodos empiezan a resincronizar, EL lider analogo se pone en True
    y todos miran a su predecesor cuando el lider vea su predecesor en True => que la red es estable para la insercción y mas de nuevo Esto es vital para el server del buscador

        
- añadir la capa de seguridad que durante la inserccion en todo momento revise que no se está en eleccion 

-X Hay que hacer el tercer check y añadir el simulador 


- En la parte de corregir el sucesor primero convocar elecciones en caso de no verlo
- No se arreglan todo el tiempo los fingers poner que se tienen que arreglar solo despues de estabilizar
- Estabilizar es cuando la lista de sucesores y finger table esté estable
