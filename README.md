# OrderFlow

Plataforma de procesamiento de **órdenes y pagos orientada a eventos** (event-driven), con **saga orquestada**, **transactional outbox** y **compensaciones**. Proyecto de portafolio *production-style*.

- 📐 Especificación: [docs/SDD.md](docs/SDD.md)
- 🧰 Stack tecnológico (versiones pinned): [docs/TECH-STACK.md](docs/TECH-STACK.md)
- 🛠️ Guía de desarrollo (build, infra, convenciones): [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)

## Stack

Java 25 · Spring Boot 4.0 · Apache Kafka (KRaft) + Confluent Schema Registry (Avro) · PostgreSQL 16 · Redis 7 · Resilience4j · OpenTelemetry + Grafana · Maven multi-módulo · Testcontainers.

## Estructura del repositorio

```
orderflow/
├── pom.xml                 # POM padre (BOMs, toolchains, plugins)
├── toolchains.xml          # fija JDK 25 para la compilación
├── docker-compose.yml      # infra local (Kafka, Schema Registry, Postgres, Redis, AKHQ, observabilidad)
├── infra/                  # init scripts (p. ej. creación de bases Postgres)
├── docs/                   # SDD.md, TECH-STACK.md
├── common-lib/             # utilidades compartidas (outbox, idempotencia, tracing) — F1+
├── events-schema/          # esquemas Avro + clases generadas
├── order-service/          # agregado Order + Saga Orchestrator (REST)
├── payment-service/        # pagos (gateway simulado, idempotente)
├── inventory-service/      # reserva/liberación de stock
└── notification-service/   # notificaciones ante eventos terminales
```

## Prerrequisitos

- **JDK 25** (cualquier distribución OpenJDK).
- **Maven 3.9+** (o usar el wrapper `./mvnw`, incluido en el repo).
- **Docker Desktop** (solo para levantar la infra; no hace falta para compilar).

## Build

1. Crea tu `toolchains.xml` a partir de la plantilla y ajusta la ruta a tu JDK 25:

   ```bash
   cp toolchains.xml.example toolchains.xml   # luego edita <jdkHome>
   ```

2. Compila (el JDK 25 se fija vía toolchain, independiente del `PATH`):

   ```bash
   ./mvnw -t toolchains.xml clean verify
   ```

> Maven debe ejecutarse con un JDK 17+ en `JAVA_HOME`; la *compilación* usa el JDK 25 del toolchain.

## Infra local

```bash
docker compose up -d
```

| Recurso | URL / Puerto |
|---|---|
| Kafka (bootstrap) | `localhost:9092` |
| Schema Registry | `http://localhost:8085` |
| PostgreSQL | `localhost:5432` (user/pass `orderflow`) |
| Redis | `localhost:6379` |
| AKHQ (UI Kafka) | `http://localhost:8088` |
| Grafana | `http://localhost:3000` |

Los topics `.v1` (6 particiones) y sus `.dlq` se crean automáticamente al arrancar (`kafka-init`).

## Servicios y puertos

| Servicio | Puerto | Estado F0 |
|---|---|---|
| order-service | 8081 | esqueleto (web + actuator) |
| payment-service | 8082 | esqueleto |
| inventory-service | 8083 | esqueleto |
| notification-service | 8084 | esqueleto |

## Roadmap

**F0 scaffolding ✅** → F1 crear orden + outbox → F2 saga (inventario+pago) → F3 compensaciones → F4 idempotencia + DLQ → F5 observabilidad → F6 carga + CI. Detalle en [SDD §8.1](docs/SDD.md).
