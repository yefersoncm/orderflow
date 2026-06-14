# Stack Tecnológico — OrderFlow

| Campo | Valor |
|---|---|
| **Proyecto** | OrderFlow — Plataforma de procesamiento de órdenes y pagos orientada a eventos |
| **Documento** | Definición de Stack Tecnológico |
| **Versión** | 1.0.0 |
| **Fecha** | 2026-06-14 |
| **Estado** | Definido |
| **Relacionado** | [SDD v0.2.0](./SDD.md) — este documento concreta las versiones del stack canónico del SDD |
| **Plataforma base** | **Java 25 (LTS)** + **Spring Boot 4.0.x** |

> **Estrategia de versionado:** se gobiernan las versiones mediante **BOMs** (Spring Boot 4.0, Spring Cloud 2025.1 Oakwood, Testcontainers). Las dependencias gestionadas por un BOM **NO se pinnean a mano** (se heredan); solo se fijan explícitamente las que quedan **fuera de BOM** (Confluent, Avro, Redisson, imágenes Docker). La versión de Java se fija con `maven-toolchains-plugin`, independiente del `PATH`.

---

## 0. Decisión base y compatibilidad

- **Java 25 (LTS)** — `25.0.2` instalado en `<RUTA_A_TU_JDK_25>` (toolchain). Virtual Threads (GA desde 21), Scoped Values (finalizado en 25).
- **Spring Boot 4.0.6** (último estable, abr-2026) sobre **Spring Framework 7.0.7**, **Jakarta EE 11**, **Jackson 3**, **JSpecify**. Soporte *first-class* de Java 25.
- **Spring Cloud 2025.1.2 (Oakwood)** — release train alineado con Boot 4.0.x.

Verificado contra fuentes de jun-2026 (ver [§12](#12-fuentes)).

---

## 1. Plataforma y build

| Componente | Coordenada / Artefacto | Versión | Notas |
|---|---|---|---|
| JDK | OpenJDK (Temurin) | **25.0.2** | Fijado por toolchain, no por PATH |
| Build tool | Apache Maven (multi-módulo) | **3.9.x** | Vía Maven Wrapper (`./mvnw`); Maven 4 opcional a futuro |
| Toolchain | `org.apache.maven.plugins:maven-toolchains-plugin` | 3.x | Apunta a JDK 25 (`toolchains.xml`) |
| Empaquetado | `org.springframework.boot:spring-boot-maven-plugin` | *(BOM Boot 4.0)* | `build-image` (Buildpacks) → imágenes OCI |
| Codegen Avro | `org.apache.avro:avro-maven-plugin` | **1.12.x** | `generate-sources`; alinear con la versión de Avro del serializer Confluent |
| Formato | `com.diffplug.spotless:spotless-maven-plugin` | 2.x | `google-java-format` |
| Cobertura | `org.jacoco:jacoco-maven-plugin` | 0.8.x | *Gates* de cobertura (SDD RNF-32) |
| Tests | `maven-surefire-plugin` / `maven-failsafe-plugin` | *(BOM)* | Unitarias / integración |

**`toolchains.xml` (esqueleto):**
```xml
<toolchains>
  <toolchain>
    <type>jdk</type>
    <provides><version>25</version><vendor>openjdk</vendor></provides>
    <configuration>
      <jdkHome><RUTA_A_TU_JDK_25></jdkHome>
    </configuration>
  </toolchain>
</toolchains>
```

---

## 2. Framework de aplicación — Spring Boot 4.0.x

| Dependencia | Coordenada | Gestión | Uso |
|---|---|---|---|
| Core web/REST | `org.springframework.boot:spring-boot-starter-web` | BOM | API REST de `order-service` |
| Validación | `org.springframework.boot:spring-boot-starter-validation` | BOM | Validación de payloads (Jakarta Validation) |
| Actuator | `org.springframework.boot:spring-boot-starter-actuator` | BOM | Health, readiness, métricas |
| Spring Framework | `org.springframework:*` | BOM → **7.0.7** | Núcleo |
| Serialización JSON | **Jackson 3** | BOM | **⚠ nuevo namespace `tools.jackson.*`** (ver [§11](#11-riesgos-de-compatibilidad)) |
| Null-safety | **JSpecify** | BOM | Anotaciones `@Nullable`/`@NonNull` estandarizadas |

> Boot 4 modulariza el código en jars más pequeños y estrena **versionado de API HTTP** estable (útil si exponemos `v1`/`v2` de la API REST).

---

## 3. Mensajería y esquemas

| Dependencia | Coordenada | Versión | Notas |
|---|---|---|---|
| Cliente Kafka | `org.apache.kafka:kafka-clients` | *(BOM, 4.x)* | Vía Spring Kafka |
| Spring Kafka | `org.springframework.kafka:spring-kafka` | *(BOM → **4.0.5**)* | Productores/consumidores, `DefaultErrorHandler`, DLQ |
| Serializer Avro | `io.confluent:kafka-avro-serializer` | **8.2.1** | **Fuera de BOM**; repo `packages.confluent.io` |
| Schema Registry client | `io.confluent:kafka-schema-registry-client` | **8.2.x** | Alinear con el serializer |
| Avro | `org.apache.avro:avro` | **1.12.x** | Clases generadas desde `.avsc` |

**Repositorio Maven extra (Confluent, fuera de Maven Central):**
```xml
<repository>
  <id>confluent</id>
  <url>https://packages.confluent.io/maven/</url>
</repository>
```

- **Topics, claves y particiones**: según SDD §4.3 (key = `orderId`, 6 particiones, `.v1`, `.dlq`).
- **Compatibilidad de esquemas**: `BACKWARD` en Schema Registry (SDD §5.5).

---

## 4. Persistencia y caché

| Dependencia | Coordenada | Gestión | Uso |
|---|---|---|---|
| JPA / ORM | `spring-boot-starter-data-jpa` (Hibernate ORM 7.x) | BOM | Agregados, outbox, `processed_messages` |
| Driver PostgreSQL | `org.postgresql:postgresql` | BOM | Una BD por servicio |
| Migraciones | `org.flywaydb:flyway-core` + `flyway-database-postgresql` | BOM | DDL versionado (outbox, esquema por servicio) |
| Redis | `spring-boot-starter-data-redis` (Lettuce) | BOM | Idempotencia/dedup, caché |
| Locks distribuidos *(opcional)* | `org.redisson:redisson-spring-boot-starter` | **3.x** *(fuera de BOM)* | Locks robustos; alternativa a `SET NX` manual |

> Decisión: **Flyway** sobre Liquibase (SQL-first, más simple para este alcance). Redis con **Lettuce** (default de Boot); **Redisson** solo si necesitamos locks distribuidos con semántica fuerte.

---

## 5. Resiliencia

| Dependencia | Coordenada | Gestión | Notas |
|---|---|---|---|
| Circuit Breaker / Retry | `org.springframework.cloud:spring-cloud-starter-circuitbreaker-resilience4j` | **BOM Spring Cloud 2025.1** | **Vía recomendada en Boot 4** |
| Resilience4j (núcleo) | `io.github.resilience4j:*` | *(transitivo del starter Cloud)* | CB, retry, rate limiter, bulkhead |

> **⚠ Importante (ver [§11](#11-riesgos-de-compatibilidad)):** en Spring Boot 4 conviene integrar Resilience4j **a través de Spring Cloud Circuit Breaker (2025.1)**, no con el starter standalone `resilience4j-spring-boot3` (nombrado para Boot 3 y rezagado en Boot 4). Spring Framework 7 además trae `@Retryable` y *concurrency throttling* nativos que pueden complementar.

---

## 6. Observabilidad

| Dependencia | Coordenada | Gestión | Uso |
|---|---|---|---|
| Tracing + OTLP | `org.springframework.boot:spring-boot-starter-opentelemetry` | BOM | **Nuevo en Boot 4**: OTel API + bridge Micrometer + exportadores OTLP (trazas y métricas) |
| Métricas Prometheus | `io.micrometer:micrometer-registry-prometheus` | BOM | Endpoint `/actuator/prometheus` |
| Logs estructurados | Logback (structured logging de Boot 4) | BOM | JSON con `traceId`/`spanId`/`correlationId` |

- **Propagación**: W3C Trace Context, `traceId` viaja en headers de Kafka extremo a extremo (SDD §3.5, §4.5).
- **Backend local**: ver [§9](#9-infraestructura-local-docker-compose) — se recomienda `grafana/otel-lgtm` (todo-en-uno) para simplicidad.

---

## 7. Testing

| Dependencia | Coordenada | Versión | Uso |
|---|---|---|---|
| Test framework | `org.springframework.boot:spring-boot-starter-test` (JUnit 5) | BOM | Base de pruebas |
| Testcontainers BOM | `org.testcontainers:testcontainers-bom` | **2.0.5** | Importar para gestionar módulos |
| TC módulos | `org.testcontainers:{kafka,postgresql}` | *(BOM TC)* | Infra real en integración |
| Integración Boot | `org.springframework.boot:spring-boot-testcontainers` | BOM | `@ServiceConnection` |
| Kafka test | `org.springframework.kafka:spring-kafka-test` | BOM | Listeners/consumidores |
| Asincronía | `org.awaitility:awaitility` | BOM | Saga E2E (esperar estados terminales) |
| Contract testing | `org.springframework.cloud:spring-cloud-starter-contract-*` | BOM 2025.1 | **Verificar Boot 4** — fallback: **Pact** |
| Carga | **k6** (o **Gatling**) | latest | Externo a Maven; targets de RNF (SDD §7.7) |

---

## 8. Calidad y CI/CD

| Herramienta | Artefacto / Acción | Uso |
|---|---|---|
| Formato | Spotless + `google-java-format` | Estilo consistente (gate) |
| Análisis estático | SpotBugs (o SonarQube) | 0 issues bloqueantes (SDD RNF-34) |
| Seguridad deps | OWASP `dependency-check` / **Trivy** | Escaneo de vulnerabilidades |
| Cobertura | JaCoCo | Gate ≥70% global / ≥85% dominio (SDD RNF-32) |
| CI | GitHub Actions: `actions/setup-java@v4` (**Temurin 25**) + `maven-toolchains` | `mvn -B verify` |
| Imagen | `spring-boot:build-image` (Buildpacks) → **GHCR** | Sin Dockerfile a mano |

---

## 9. Infraestructura local (Docker Compose)

| Servicio | Imagen | Tag | Notas |
|---|---|---|---|
| Kafka (KRaft) | `confluentinc/cp-kafka` | **8.x** | Sin ZooKeeper |
| Schema Registry | `confluentinc/cp-schema-registry` | **8.x** | Alinear 8.x con el serializer 8.2.x |
| PostgreSQL ×4 | `postgres` | **16** | Una instancia/esquema por servicio |
| Redis | `redis` | **7** | Idempotencia/locks |
| UI Kafka | `tchiotludo/akhq` | latest | Inspección de topics/DLQ |
| Observabilidad | `grafana/otel-lgtm` | latest | Todo-en-uno: Grafana + Tempo + Prometheus/Mimir + OTel Collector |

> Alternativa de observabilidad "por piezas": `prom/prometheus` + `grafana/grafana` + `grafana/tempo` por separado (más control, más YAML). Para desarrollo, `otel-lgtm` arranca todo con un contenedor.

---

## 10. Estrategia de BOMs (qué se hereda vs qué se fija)

```xml
<dependencyManagement>
  <dependencies>
    <!-- Hereda Spring Framework 7, Jackson 3, Spring Kafka 4.0.5, Micrometer, JUnit, Flyway, Lettuce... -->
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-dependencies</artifactId>
      <version>4.0.6</version><type>pom</type><scope>import</scope>
    </dependency>
    <!-- Resilience4j (Circuit Breaker), Contract... -->
    <dependency>
      <groupId>org.springframework.cloud</groupId>
      <artifactId>spring-cloud-dependencies</artifactId>
      <version>2025.1.2</version><type>pom</type><scope>import</scope>
    </dependency>
    <!-- Módulos de Testcontainers -->
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>testcontainers-bom</artifactId>
      <version>2.0.5</version><type>pom</type><scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

**Se fija explícitamente (fuera de BOM):** `kafka-avro-serializer`/`kafka-schema-registry-client` (8.2.x), `avro` + `avro-maven-plugin` (1.12.x), `redisson` (si se usa), y los **tags de imágenes Docker**.

---

## 11. Riesgos de compatibilidad

| # | Riesgo | Impacto | Mitigación |
|---|---|---|---|
| R1 | **Jackson 2 → 3** (nuevo namespace `tools.jackson.*`) | Imports/serializadores custom | La autoconfig de Boot 4 cubre el grueso; migrar solo serializadores/mixins propios |
| R2 | **Resilience4j en Boot 4** | El starter standalone va rezagado | Usar `spring-cloud-starter-circuitbreaker-resilience4j` (2025.1); seguir issue resilience4j #2351 |
| R3 | **Spring Cloud Contract en Boot 4** | Incierto | Verificar en 2025.1; si hay fricción, **Pact** como alternativa |
| R4 | **Confluent serializer ↔ Avro** | Mismatch de versión de Avro | Alinear `avro` con la que trae el serializer 8.2.x; repo `packages.confluent.io` |
| R5 | **Virtual Threads + pinning** | JDBC/Lettuce bajo carga | Medir en F6; preferir locks de `java.util.concurrent`; pool dedicado si hace falta |
| R6 | **Ecosistema SF7/Jakarta 11** | Libs adaptándose | Preferir starters oficiales gestionados por BOM; validar terceros |

---

## 12. Fuentes

- [Spring Boot 4.0.0 available now — spring.io](https://spring.io/blog/2025/11/20/spring-boot-4-0-0-available-now/)
- [Spring Cloud 2025.1.2 (Oakwood) released — spring.io](https://spring.io/blog/2026/06/11/spring-cloud-2025-1-2-aka-oakwood-has-been-released/)
- [System Requirements :: Spring Boot — docs.spring.io](https://docs.spring.io/spring-boot/system-requirements.html)
- [OpenTelemetry with Spring Boot — spring.io](https://spring.io/blog/2025/11/18/opentelemetry-with-spring-boot/)
- [Spring Boot versions & EOL (abr-2026) — HeroDevs](https://www.herodevs.com/blog-posts/spring-boot-versions-eol-dates-and-latest-releases-april-2026)
- [Testcontainers BOM — Maven Repository](https://mvnrepository.com/artifact/org.testcontainers/testcontainers-bom)
- [io.confluent:kafka-avro-serializer — Maven Repository](https://mvnrepository.com/artifact/io.confluent/kafka-avro-serializer)
- [Resilience4j — Spring Boot 4 Compatibility (issue #2351)](https://github.com/resilience4j/resilience4j/issues/2351)

---

## Control de cambios

| Versión | Fecha | Cambios |
|---|---|---|
| 1.0.0 | 2026-06-14 | Definición inicial del stack sobre Java 25 + Spring Boot 4.0.6 (Spring Cloud 2025.1 Oakwood). Versiones verificadas con fuentes de jun-2026. |
