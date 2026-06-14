# Guía de desarrollo — OrderFlow

Onboarding y flujo de trabajo para desarrollar OrderFlow. Para el **diseño** ver [SDD.md](SDD.md); para las **versiones** ver [TECH-STACK.md](TECH-STACK.md).

## Estado del proyecto

Roadmap por fases (*vertical slices*, [SDD §8.1](SDD.md)):

- [x] **F0 — Scaffolding + infra** · monorepo Maven (6 módulos), build verde con JDK 25, `docker-compose` con auto-creación de topics. ✅
- [ ] **F1 — Crear orden + outbox** · `POST /orders`, persistencia JPA, tabla *outbox*, evento `OrderCreated`.
- [ ] **F2 — Saga (camino feliz)** · orquestador, reserva de inventario + cobro de pago.
- [ ] **F3 — Compensaciones** · ramas de fallo, `ReleaseInventory` / `OrderCancelled`, notificaciones.
- [ ] **F4 — Idempotencia + DLQ + resiliencia** · dedup, reintentos con backoff, dead-letter, circuit breaker.
- [ ] **F5 — Observabilidad** · OpenTelemetry, métricas, dashboards, tracing extremo a extremo.
- [ ] **F6 — Carga + CI** · pruebas de carga (k6/Gatling) y pipeline en GitHub Actions.

## Prerrequisitos

| Herramienta | Versión | Notas |
|---|---|---|
| JDK | **25** | Fijado por toolchain durante el build |
| Maven | 3.9+ | O usar el wrapper `./mvnw` (incluido) |
| Docker Desktop | — | Solo para levantar la infra local |

## Compilar

1. Crea tu `toolchains.xml` desde la plantilla y ajusta la ruta a tu JDK 25:
   ```bash
   cp toolchains.xml.example toolchains.xml   # edita <jdkHome>
   ```
2. Compila todo el reactor (el JDK 25 se fija vía toolchain, sin depender del `PATH`):
   ```bash
   ./mvnw -t toolchains.xml clean verify
   ```

> Maven se ejecuta con el JDK que tengas en `JAVA_HOME` (17+); la *compilación* la realiza el JDK 25 del toolchain. En este equipo (Windows) puedes activar el 25 con la función de perfil `j25`.

## Levantar la infraestructura

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

Los topics `.v1` (6 particiones) y sus `.dlq` se crean automáticamente al arrancar (servicio `kafka-init`).

## Ejecutar un servicio

```bash
# Tras 'verify', desde el jar ejecutable:
java -jar order-service/target/order-service-0.1.0-SNAPSHOT.jar

# O con el plugin (recompila):
./mvnw -t toolchains.xml -pl order-service spring-boot:run
```

| Servicio | Puerto | Health |
|---|---|---|
| order-service | 8081 | `/actuator/health` |
| payment-service | 8082 | `/actuator/health` |
| inventory-service | 8083 | `/actuator/health` |
| notification-service | 8084 | `/actuator/health` |

## Estructura del repositorio

```
common-lib/        utilidades compartidas (outbox, idempotencia, tracing) — se llena en F1+
events-schema/     esquemas Avro (.avsc) + clases generadas
<svc>-service/     apps Spring Boot por bounded context
infra/             init scripts de infraestructura (p. ej. bases Postgres)
docs/              SDD · TECH-STACK · esta guía
```

## Convenciones

- **Paquetes:** `com.orderflow.<servicio>`.
- **Eventos / comandos / topics:** usar el **catálogo canónico** del [SDD §5.3](SDD.md). Topics `*.v1`, key = `orderId`, dead-letter `*.dlq`.
- **Esquemas Avro:** en `events-schema/src/main/avro`; compatibilidad **BACKWARD** en Schema Registry.
- **Semántica de mensajería:** at-least-once + consumidores idempotentes (no exactly-once); ver ADR-03/04 del SDD.
- **Commits:** Conventional Commits (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`…).
- **Ramas:** una rama por fase/slice → Pull Request a `main`.

## Troubleshooting

| Síntoma | Causa / solución |
|---|---|
| `release 25` no reconocido o compila con otra versión | Falta el toolchain: pasa `-t toolchains.xml` y revisa `<jdkHome>`. |
| El build arranca con otro JDK | `JAVA_HOME` debe apuntar a un JDK 17+ para *ejecutar* Maven; la compilación la hace el toolchain (25). |
| Avro no genera clases | Ejecuta `./mvnw -pl events-schema generate-sources` y revisa `target/generated-sources/avro`. |
| La infra no conecta | Verifica que `docker compose ps` muestre todo *healthy*; Kafka tarda unos segundos en estar listo. |
