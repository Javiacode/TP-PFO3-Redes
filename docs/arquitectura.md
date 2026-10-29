# Arquitectura del sistema

El diagrama muestra la arquitectura distribuida objetivo y separa lo que ejecuta esta entrega de los componentes planteados para un despliegue distribuido completo.

```mermaid
flowchart LR
    subgraph Implementado["Prototipo ejecutable en este repositorio"]
        CLI["Cliente Python por consola"]
        TCP["Servidor TCP en Python"]
        Pool["Pool acotado de hilos workers"]
        CLI -->|"JSON por TCP"| TCP
        TCP -->|"envia tarea"| Pool
        Pool -->|"resultado o error"| TCP
        TCP -->|"respuesta JSON"| CLI
    end

    subgraph Objetivo["Arquitectura distribuida objetivo - no desplegada por este prototipo"]
        Mobile["Clientes moviles"]
        Web["Clientes web"]
        LB["Balanceador TCP: Nginx Stream o HAProxy"]
        W1["Worker server 1<br/>pool de hilos"]
        W2["Worker server 2<br/>pool de hilos"]
        MQ[("RabbitMQ<br/>cola entre servidores")]
        PG[("PostgreSQL<br/>datos estructurados")]
        S3[("S3<br/>archivos y objetos")]

        Mobile --> LB
        Web --> LB
        LB --> W1
        LB --> W2
        W1 <--> MQ
        W2 <--> MQ
        W1 --> PG
        W2 --> PG
        W1 --> S3
        W2 --> S3
    end

    classDef implemented fill:#d9f2e6,stroke:#217a4b,color:#102b1d,stroke-width:2px
    classDef proposed fill:#fff0cf,stroke:#a66b00,color:#3b2b0a,stroke-dasharray:5 5
    class CLI,TCP,Pool implemented
    class Mobile,Web,LB,W1,W2,MQ,PG,S3 proposed
```

## Componentes

| Componente | Rol | Estado en esta entrega |
| --- | --- | --- |
| Cliente Python | Envía una tarea y espera la respuesta por TCP | Implementado |
| Servidor TCP | Recibe mensajes JSON y coordina su procesamiento | Implementado |
| Pool de hilos | Ejecuta tareas en paralelo con un limite de workers | Implementado |
| Nginx Stream o HAProxy | Distribuye conexiones TCP entre instancias | Arquitectura objetivo |
| RabbitMQ | Coordina trabajo asincrono entre servidores | Arquitectura objetivo |
| PostgreSQL | Persiste datos estructurados compartidos | Arquitectura objetivo |
| S3 | Almacena archivos u objetos compartidos | Arquitectura objetivo |

El prototipo no necesita servicios externos: modela la distribucion local con sockets TCP y un `ThreadPoolExecutor`. Para escalarlo, se desplegarian varias instancias del servidor detras del balanceador y se agregarian RabbitMQ y almacenamiento compartido.
