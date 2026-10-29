# Procesador distribuido de tareas por sockets

Prototipo en Python para enviar tareas a un servidor TCP, distribuirlas entre workers concurrentes y recibir sus resultados. No depende del proyecto PFO2.

El [diagrama de arquitectura](docs/arquitectura.md) incluye la topologia distribuida solicitada. El codigo ejecutable implementa el cliente TCP, el servidor y su pool de hilos; Nginx/HAProxy, RabbitMQ, PostgreSQL y S3 se documentan como componentes de la arquitectura objetivo, pero no se despliegan en esta entrega.

## Requisitos

- Python 3.10 o superior
- No requiere paquetes externos

## Ejecucion rapida

1. Inicia el servidor en una terminal:

   ```bash
   python server.py --host 127.0.0.1 --port 5050 --workers 4
   ```

2. En otra terminal, envia una tarea de suma:

   ```bash
   python client.py --host 127.0.0.1 --port 5050 --task sum --payload "[12, 8, 5]"
   ```

3. El cliente recibe una respuesta correlacionada por identificador:

   ```json
   {
     "id": "...",
     "ok": true,
     "result": 25
   }
   ```

El servidor escucha en `0.0.0.0:5050` por defecto. Para conectar desde otra maquina, usa la IP del servidor como `--host` en el cliente y permite el puerto TCP `5050` en el firewall.

## Tareas de demostracion

| Tarea | Payload | Resultado |
| --- | --- | --- |
| `echo` | Cualquier valor JSON | Devuelve el mismo valor |
| `uppercase` | Una cadena | Devuelve la cadena en mayusculas |
| `sum` | Lista de numeros finitos | Devuelve la suma |

Ejemplos:

```bash
python client.py --task uppercase --payload '"redes distribuidas"'
python client.py --task echo --payload '{"mensaje":"hola"}'
```

En PowerShell, usa comillas simples alrededor de objetos JSON para preservar sus comillas dobles:

```powershell
python client.py --task echo --payload '{"mensaje":"hola"}'
```

## Protocolo TCP

Cada conexion transporta un mensaje JSON terminado en salto de linea y recibe una respuesta con el mismo `id`. El servidor procesa una tarea por conexion.

Solicitud:

```json
{"id":"job-123","task":"sum","payload":[1,2,3]}
```

Respuesta exitosa:

```json
{"id":"job-123","ok":true,"result":6}
```

Respuesta de error:

```json
{"id":"job-123","ok":false,"error":{"code":"unknown_task","message":"Unsupported task: missing."}}
```

El servidor limita el tamano de cada solicitud a 64 KiB, el numero de conexiones simultaneas y la cantidad de workers. Si alcanza el limite de conexiones, responde con `server_busy`. Cada solicitud valida su tarea y payload antes de devolver un resultado.

## Pruebas

Ejecuta las pruebas unitarias y de integracion desde la raiz del proyecto:

```bash
python -m unittest discover -s tests -v
```

Las pruebas cubren el procesamiento de tareas, el intercambio cliente-servidor, solicitudes concurrentes, JSON invalido, numeros no finitos y solicitudes demasiado grandes.

## Alcance de la arquitectura

El servidor puede ejecutarse como una instancia TCP independiente. En un despliegue completo, Nginx Stream o HAProxy repartirian conexiones entre varias instancias; RabbitMQ coordinaria tareas entre servidores; PostgreSQL almacenaria datos estructurados y S3 alojaria objetos. Esos servicios son parte del diagrama de referencia y no son dependencias del prototipo ejecutable.
