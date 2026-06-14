# SDD — OrderFlow: Plataforma de procesamiento de órdenes y pagos orientada a eventos

| Campo | Valor |
|---|---|
| **Proyecto** | OrderFlow — Plataforma de procesamiento de órdenes y pagos orientada a eventos |
| **Documento** | Software Design Document (SDD) |
| **Versión del SDD** | 0.3.0 |
| **Fecha** | 2026-06-14 |
| **Estado** | Draft |
| **Autor** | Yeferson Córdoba (@yefersoncm) |
| **Metodología** | Spec-Driven Development (SDD) |

> **¿Qué es un SDD y cómo guía la implementación?** Un Software Design Document es la fuente única de verdad del diseño: traduce la intención de negocio en decisiones técnicas verificables **antes** de escribir código de producción. Bajo Spec-Driven Development, la especificación precede al código, es contractual y trazable, y sus nombres/números son canónicos: si el código y este SDD discrepan, **gana el SDD** hasta que se actualice formalmente la especificación.

---

## Tabla de contenidos

1. [Introducción, visión y objetivos](#1-introducción-visión-y-objetivos)
2. [Actores, casos de uso y requisitos funcionales](#2-actores-casos-de-uso-y-requisitos-funcionales)
3. [Requisitos no funcionales](#3-requisitos-no-funcionales)
4. [Arquitectura del sistema](#4-arquitectura-del-sistema)
5. [Modelo de dominio, contratos de eventos y APIs](#5-modelo-de-dominio-contratos-de-eventos-y-apis)
6. [Decisiones de arquitectura (ADRs)](#6-decisiones-de-arquitectura-adrs)
7. [Estrategia de pruebas y criterios de aceptación](#7-estrategia-de-pruebas-y-criterios-de-aceptación)
8. [Roadmap, riesgos y operabilidad](#8-roadmap-riesgos-y-operabilidad)
9. [Anexos / Referencias](#9-anexos--referencias)
10. [Control de cambios del documento](#10-control-de-cambios-del-documento)

---

## 1. Introducción, visión y objetivos

### 1.1. Propósito de este SDD y su uso en Spec-Driven Development

Este SDD es la **fuente única de verdad (single source of truth)** del diseño de OrderFlow. Su propósito es **traducir intención de negocio en decisiones técnicas verificables** antes de escribir código de producción, de modo que la implementación sea una consecuencia de la especificación y no al revés.

Bajo el enfoque **Spec-Driven Development (SDD)**, este documento se usa así:

1. **La especificación precede al código.** Cada artefacto relevante (saga, evento, esquema Avro, contrato REST, topología de Kafka) se describe aquí antes de implementarse.
2. **Es contractual y trazable.** Cada objetivo (`OBJ-NN`), evento y comando del catálogo es referenciable desde el código, los tests y los commits. Los nombres y números de este documento son **canónicos**: el código se ajusta al SDD, no al contrario.
3. **Es ejecutable como criterio de aceptación.** Los objetivos medibles ([§1.4](#14-objetivos-medibles-del-proyecto)) se materializan en pruebas automatizadas (Testcontainers, contract testing, k6/Gatling). "Hecho" significa "cumple el objetivo verificable", no "compila".
4. **Es vivo y versionado.** El SDD evoluciona con SemVer propio (hoy `0.1.0`, estado *Draft*). Cambios de diseño se reflejan primero aquí; la divergencia código-spec se trata como defecto.

**Audiencia:** ingeniería (implementación), revisores de portafolio y el futuro yo (mantenimiento). **Alcance del documento:** diseño técnico de referencia para construir OrderFlow de extremo a extremo.

> Regla operativa: si el código y el SDD discrepan, **gana el SDD** hasta que se actualice formalmente la especificación.

### 1.2. Visión del producto y planteamiento del problema

**Visión.** OrderFlow gestiona el **ciclo de vida completo de una orden** — creación → reserva de inventario → cobro → confirmación/envío — de forma **asíncrona, resiliente y trazable**, aplicando **compensaciones automáticas** ante fallos parciales. Es un proyecto de portafolio *production-style*: construible por una sola persona senior, pero con los patrones, la observabilidad y las garantías de un sistema real.

**El problema.** Procesar una orden no es una operación atómica: toca **inventario**, **pagos** y **notificación**, cada uno con su propia base de datos, su propia disponibilidad y su propia latencia. Modelarlo como un CRUD síncrono fuerza una transacción distribuida implícita sobre HTTP encadenado.

**Por qué event-driven y no un CRUD síncrono:**

| Dimensión | CRUD síncrono (HTTP encadenado) | Event-driven + Saga (OrderFlow) |
|---|---|---|
| **Acoplamiento** | Temporal: si pagos cae, la creación de la orden falla | Desacoplado: los servicios se comunican por eventos; un consumidor caído se reanuda |
| **Atomicidad cross-service** | Requiere 2PC o queda inconsistente ante fallo parcial | Saga orquestada con **compensaciones** (consistencia eventual explícita) |
| **Resiliencia** | El cliente espera la cadena completa; un timeout aborta todo | Retry/backoff, DLQ y circuit breakers aíslan el fallo |
| **Throughput / latencia** | Limitado por el eslabón más lento; bloqueante | Paralelismo entre órdenes; particionado por `orderId` |
| **Trazabilidad de estado** | Estado disperso e implícito en llamadas | Historia de eventos explícita y auditable (event log) |
| **Pérdida de datos** | "Doble escritura" (BD + llamada) puede perder eventos | **Transactional Outbox** ⇒ cero pérdida (at-least-once) |

La decisión arquitectónica nuclear es que **el dominio es intrínsecamente distribuido y asíncrono**; modelarlo con eventos, una saga orquestada y el patrón outbox refleja la realidad del problema en lugar de ocultarla tras una fachada síncrona frágil.

### 1.3. Alcance del proyecto

#### Qué SÍ cubre (in-scope)

- **Saga orquestada completa** del ciclo de la orden (`PENDING → CONFIRMED | CANCELLED`) con su camino feliz y todas las compensaciones.
- **Cuatro servicios** con bounded context y BD propia: `order-service` (agregado Order + orquestador), `payment-service`, `inventory-service`, `notification-service`.
- **Mensajería Kafka (KRaft)** + **Schema Registry con Avro**, topología de topics `.v1` y DLQ por topic consumido.
- **Patrones de fiabilidad**: Transactional Outbox + relay, consumidores idempotentes (dedup), DLQ con retry y backoff exponencial, particionado por `orderId`.
- **Observabilidad de extremo a extremo**: trazas OpenTelemetry, métricas Micrometer/Prometheus, dashboards Grafana, logging JSON con `traceId`/`correlationId`.
- **Infra local reproducible** vía Docker Compose y un único comando de build.
- **Pruebas**: unitarias, de integración con Testcontainers, contract testing y carga (k6/Gatling).
- **CI** en GitHub Actions (build, test, escaneo, build de imagen).

#### Qué NO cubre (non-goals)

- **No** es un e-commerce real: sin catálogo de productos rico, carrito, búsqueda, precios dinámicos ni impuestos/envíos reales.
- **No** integra **gateways de pago reales**: el cobro es un *gateway simulado* (con latencia y fallos inyectables).
- **No** persigue **exactly-once nativo**: se asume **at-least-once + idempotencia** (justificado en [§1.5](#15-supuestos-y-restricciones) y [ADR-03](#adr-03-semántica-at-least-once--consumidores-idempotentes-vs-exactly-once)).
- **No** incluye **autenticación/autorización de usuarios finales**, gestión de cuentas ni UI de cliente (sí una protección básica de la API, ver [RNF-30](#36-seguridad)).
- **No** aborda **despliegue en la nube ni Kubernetes/Helm**: el objetivo es ejecución local reproducible (CD a registro de imágenes queda fuera, salvo el push opcional descrito en [§8.6](#86-pipeline-cicd-github-actions)).
- **No** cubre **multi-tenancy**, internacionalización, ni cumplimiento normativo (PCI-DSS, GDPR) más allá de buenas prácticas.
- **No** implementa **CDC/Debezium**: se menciona como alternativa al relay poll-publisher, pero la implementación canónica es el relay tipo *poll-and-publish* (ver [ADR-02](#adr-02-transactional-outbox-vs-publicación-directa)).

### 1.4. Objetivos medibles del proyecto

Cada objetivo es **verificable** mediante prueba automatizada o medición instrumentada. Son borrador y ajustables, pero canónicos como referencia. Se refinan como RNF en la [sección 3](#3-requisitos-no-funcionales) y como criterios de aceptación en la [sección 7](#79-criterios-de-aceptación-numerados).

| ID | Objetivo | Métrica / Criterio de aceptación |
|---|---|---|
| **OBJ-01** | **Throughput sostenido** del procesamiento de órdenes | ≥ **1.000 órdenes/min** sostenidas en hardware de laptop, medido con k6/Gatling |
| **OBJ-02** | **Latencia p99** de completar la saga | **< 2 s** de extremo a extremo, **excluyendo** la latencia simulada del gateway de pago |
| **OBJ-03** | **Cero pérdida de eventos** | Ningún evento de dominio se pierde; garantizado por **Transactional Outbox + at-least-once**, verificado con pruebas de fallo |
| **OBJ-04** | **Recuperación sin efectos duplicados** | La caída de un consumidor a mitad de saga se reanuda **sin** side-effects duplicados (idempotencia comprobada con Testcontainers) |
| **OBJ-05** | **Trazabilidad extremo a extremo** | **100 %** de los pasos de la saga comparten un mismo `traceId` que atraviesa los 4 servicios |
| **OBJ-06** | **Arranque local reproducible** | `docker compose up` + **un** comando de build levantan el sistema **sin pasos manuales** |
| **OBJ-07** | **Consistencia eventual correcta** | Toda saga termina en estado terminal coherente (`CONFIRMED` o `CANCELLED`) con compensaciones aplicadas; sin órdenes "colgadas" |
| **OBJ-08** | **Idempotencia de consumidores** | Reentrega del mismo mensaje (`messageId`) no duplica reservas, cobros ni notificaciones |

### 1.5. Supuestos y restricciones

**Supuestos**

- El desarrollo lo realiza **una sola persona senior**; las decisiones favorecen simplicidad operativa sobre escala extrema.
- El **hardware objetivo es una laptop**; los objetivos de rendimiento ([§1.4](#14-objetivos-medibles-del-proyecto)) se calibran a ese contexto.
- El **gateway de pago es simulado**: su latencia y tasa de fallo son inyectables para probar compensaciones.
- La carga es de naturaleza **e-commerce-like**; el orden por orden importa, pero las órdenes son independientes entre sí.

**Restricciones (canónicas, no negociables)**

- **Java 25 (LTS)** (Virtual Threads donde aplique); build **Maven multi-módulo** con `maven-toolchains-plugin` fijando Java 25 independiente del PATH ([ADR-08](#adr-08-java-25--virtual-threads-y-maven-toolchains)).
- **Spring Boot 4.0.x** (4.0.6) sobre Spring Framework 7 / Jakarta EE 11 / Jackson 3, alineado con **Spring Cloud 2025.1 (Oakwood)**; soporte *first-class* de Java 25 (+ Spring Kafka, Web, Data JPA). Versiones concretas y *pinned* en [TECH-STACK.md](./TECH-STACK.md).
- **Apache Kafka en modo KRaft** + **Confluent Schema Registry con Avro** ([ADR-07](#adr-07-avro--schema-registry-vs-json)).
- **PostgreSQL 16** (una BD por servicio + tabla `outbox`) y **Redis 7** (idempotencia/dedup y locks distribuidos) ([ADR-09](#adr-09-una-base-de-datos-por-servicio-database-per-service)).
- **Semántica de entrega at-least-once** + idempotencia (no exactly-once nativo). *Justificación:* exactly-once end-to-end cruzando múltiples brokers, bases de datos y side-effects externos (cobro real) es frágil y costoso; at-least-once con consumidores idempotentes y outbox ofrece las mismas garantías efectivas con mucho menor coste y complejidad ([ADR-03](#adr-03-semántica-at-least-once--consumidores-idempotentes-vs-exactly-once)).
- **Key de Kafka = `orderId`**, **6 particiones** por topic: orden total por orden, paralelismo entre órdenes ([ADR-06](#adr-06-particionamiento-por-orderid-para-orden)).
- **Toda escritura de estado + evento es atómica** vía outbox (prohibida la doble escritura BD/broker fuera de transacción) ([ADR-02](#adr-02-transactional-outbox-vs-publicación-directa)).

### 1.6. Glosario y lenguaje ubicuo

Vocabulario compartido (DDD *ubiquitous language*). Estos términos se usan idénticamente en código, eventos, logs y documentación.

| Término | Definición en OrderFlow |
|---|---|
| **Orden (Order)** | Agregado raíz propiedad de `order-service`. Estados: `PENDING`, `CONFIRMED`, `CANCELLED`. Identificada por `orderId` (clave de partición). |
| **Saga** | Secuencia de transacciones locales coordinadas a través de servicios, con **transacciones compensatorias** que deshacen pasos previos ante un fallo. En OrderFlow es **orquestada**. |
| **Orquestador (Saga Orchestrator)** | Componente central en `order-service` que decide el siguiente paso de la saga emitiendo **comandos** y reaccionando a **eventos**. |
| **Compensación** | Acción que revierte el efecto de un paso ya completado (p. ej. `ReleaseInventory` tras un `PaymentDeclined`). |
| **Comando (Command)** | **Intención** dirigida a un servicio para que *haga* algo. Imperativo. Ej.: `ReserveInventory`, `ProcessPayment`. Puede ser rechazado. |
| **Evento (Event)** | **Hecho** inmutable de algo que *ya ocurrió*. Pasado. Ej.: `OrderCreated`, `PaymentAuthorized`. No se rechaza, se observa. |
| **Bounded Context** | Frontera de modelo y lenguaje de un servicio; cada uno posee su BD y su porción del dominio (DDD). |
| **Transactional Outbox** | Patrón que escribe **estado + evento** en la misma transacción de BD (tabla `outbox`); un **relay** (poll-publisher) lo publica luego a Kafka de forma confiable. Evita la doble escritura. |
| **Relay (Outbox Relay)** | Proceso que lee la tabla `outbox` y publica a Kafka. Alternativa mencionada: **CDC/Debezium**. |
| **Idempotencia** | Propiedad de que procesar el **mismo mensaje** más de una vez produzca el **mismo efecto** que procesarlo una vez. Clave: `(consumerName, messageId)`. |
| **Dedup / processed_messages** | Mecanismo (tabla `processed_messages` y/o Redis) que registra mensajes ya procesados para garantizar idempotencia. |
| **DLQ (Dead-Letter Queue)** | Topic `<nombre>.dlq` donde aterriza un mensaje tras **N reintentos** fallidos, con headers de causa, para inspección/reproceso. |
| **At-least-once** | Garantía de entrega: un mensaje se entrega **una o más veces** (nunca cero). Exige idempotencia en el consumidor. |
| **Backoff exponencial** | Estrategia de reintento donde el intervalo entre intentos crece exponencialmente, antes de derivar a la DLQ. |
| **Partición / Key** | Kafka enruta por `key = orderId`; misma clave ⇒ misma partición ⇒ **orden garantizado** para esa orden. |
| **traceId / correlationId** | Identificador que viaja por todos los servicios (OpenTelemetry) para **correlacionar** una saga de extremo a extremo en trazas y logs. |
| **Circuit Breaker** | Patrón Resilience4j que corta llamadas a una dependencia degradada para evitar fallos en cascada. |

---

## 2. Actores, casos de uso y requisitos funcionales

### 2.1 Actores y stakeholders

| Actor / Stakeholder | Tipo | Rol en el sistema | Interés principal |
|---|---|---|---|
| Cliente (Customer) | Humano (externo) | Crea órdenes vía REST y consulta su estado. | Que su orden se procese o se cancele de forma fiable y trazable. |
| Sistema upstream / Frontend e-commerce | Sistema (externo) | Invoca `POST /orders` y `GET /orders/{id}` en nombre del cliente. | Respuesta síncrona rápida (acepta la orden) y consulta de estado posterior. |
| Saga Orchestrator (order-service) | Componente interno | Orquesta el ciclo de vida de la orden y las compensaciones. | Consistencia eventual del agregado Order y cierre determinista de la saga. |
| inventory-service | Componente interno | Reserva y libera stock ante comandos. | Reservas idempotentes y no sobre-vender. |
| payment-service | Componente interno | Autoriza/declina/reembolsa pagos (gateway simulado). | Cobros idempotentes y reembolsos correctos en compensación. |
| notification-service | Componente interno | Notifica sobre eventos terminales (confirmada/cancelada). | Entregar una notificación por evento terminal sin duplicados visibles. |
| Operador / SRE | Humano (interno) | Opera la plataforma, observa métricas/trazas, gestiona DLQ. | Observabilidad extremo a extremo, reproceso de mensajes en DLQ, alertas. |
| Gateway de pago (simulado) | Sistema (externo, simulado) | Devuelve resultado de autorización/captura. | (Frontera del sistema; introduce latencia/fallo simulados.) |
| Plataforma de mensajería/datos (Kafka, Postgres, Redis) | Infraestructura | Transporte de eventos/comandos, persistencia y dedup. | Disponibilidad y garantías at-least-once. |

### 2.2 Casos de uso principales

| ID | Caso de uso | Actor primario | Disparador | Resultado esperado (éxito) |
|---|---|---|---|---|
| **CU-01** | Crear orden | Cliente / Frontend | `POST /orders` | Order persistida en estado `PENDING` y evento `OrderCreated` escrito en outbox en la misma transacción; respuesta `201` con `orderId`. |
| **CU-02** | Consultar estado de orden | Cliente / Frontend | `GET /orders/{id}` | Devuelve estado actual (`PENDING`/`CONFIRMED`/`CANCELLED`) e historial de la saga; `404` si no existe. |
| **CU-03** | Reservar inventario | Orchestrator → inventory-service | Comando `ReserveInventory` (tras `OrderCreated`) | `InventoryReserved` (stock descontado) o `InventoryReservationFailed`. |
| **CU-04** | Cobrar pago | Orchestrator → payment-service | Comando `ProcessPayment` (tras `InventoryReserved`) | `PaymentAuthorized` o `PaymentDeclined`. |
| **CU-05** | Confirmar orden | Orchestrator → order-service | `PaymentAuthorized` | Order → `CONFIRMED` y evento `OrderConfirmed` (vía outbox). |
| **CU-06** | Cancelar orden con compensación | Orchestrator | `InventoryReservationFailed` **o** `PaymentDeclined` | Compensaciones ejecutadas (si aplica `ReleaseInventory` → `InventoryReleased`), Order → `CANCELLED` y `OrderCancelled`. |
| **CU-07** | Notificar evento terminal | notification-service | `OrderConfirmed` / `OrderCancelled` | `NotificationSent` (simulada/logueada) de forma idempotente. |
| **CU-08** | Gestionar mensajes en DLQ | Operador / SRE | Mensaje agotado tras N reintentos | Mensaje enrutado a `<topic>.dlq` con headers de causa; inspección y reproceso manual. |
| **CU-09** | Observar la saga extremo a extremo | Operador / SRE | Acceso a Grafana / trazas | Un mismo `traceId` atraviesa todos los servicios de la saga. |

#### CU-01 — Crear orden (flujo detallado)

```
Precondición:  catálogo de ítems válido; cliente identificado.
Flujo principal:
  1. Frontend envía POST /orders con líneas de la orden y clave de idempotencia (Idempotency-Key).
  2. order-service valida el payload.
  3. En UNA transacción: INSERT Order(PENDING) + INSERT OrderCreated en outbox.
  4. Responde 201 Created con { orderId, status: PENDING }.
Postcondición: outbox relay publicará OrderCreated (at-least-once).
Flujos alternativos:
  2a. Payload inválido         -> 400 Bad Request (no se persiste nada).
  1a. Idempotency-Key repetida -> devuelve la orden ya creada (no duplica).
```

### 2.3 Diagrama de flujo de la Saga (camino feliz + compensaciones)

```
                        POST /orders
                            |
                            v
                 [order-service: Order=PENDING]
                  (outbox: OrderCreated, misma TX)
                            |
                            v  publica OrderCreated -> Orchestrator
                  +---------------------------+
                  | cmd: ReserveInventory     |
                  +-------------+-------------+
                                |
                                v
                      [inventory-service]
                       /                 \
            InventoryReserved         InventoryReservationFailed
                   |                            |
                   v                            v
        +----------------------+      [Order=CANCELLED]
        | cmd: ProcessPayment  |       evt: OrderCancelled
        +----------+-----------+              |
                   |                          v
                   v                        (FIN: cancelada)
          [payment-service]
            /            \
   PaymentAuthorized   PaymentDeclined
        |                    |
        |                    v   COMPENSACION
        |          +--------------------------+
        |          | cmd: ReleaseInventory    |
        |          +-----------+--------------+
        |                      v
        |             [inventory-service]
        |                      |
        |               InventoryReleased
        |                      |
        |                      v
        |             [Order=CANCELLED]
        |              evt: OrderCancelled
        |                      |
        v                      v
  cmd: ConfirmOrder        (FIN: cancelada)
        |
        v
  [Order=CONFIRMED]
   evt: OrderConfirmed
        |
        v
   [notification-service] --consume OrderConfirmed/OrderCancelled--> evt: NotificationSent
        |
        v
     (FIN: confirmada / notificada)
```

> La vista detallada de la orquestación (comandos vs. eventos y compensaciones marcadas `[C]`) se desarrolla en [§4.4](#44-saga-orquestada-comandos-eventos-y-compensaciones) y el flujo paso a paso en [§4.5](#45-flujo-de-una-orden-de-extremo-a-extremo).

Notas de robustez del flujo:
- Cada paso es disparado por un evento/comando idempotente; la reentrada (re-consumo at-least-once) no produce efectos duplicados.
- La caída de cualquier consumidor a mitad de saga se reanuda desde el último offset comprometido; el estado de la saga persiste en `order-service` ([§5.1.1](#511-agregado-order-order-service)).

### 2.4 Historias de usuario clave

| ID | Historia |
|---|---|
| HU-01 | **Como** cliente, **quiero** crear una orden con una sola llamada REST y recibir confirmación inmediata de aceptación, **para** no esperar a que se complete todo el cobro y la reserva. |
| HU-02 | **Como** cliente, **quiero** consultar el estado de mi orden por su `orderId`, **para** saber si fue confirmada o cancelada y por qué. |
| HU-03 | **Como** cliente, **quiero** que si el pago se rechaza no se me cobre y el stock se libere, **para** no quedar con una reserva fantasma ni un cargo indebido. |
| HU-04 | **Como** Saga Orchestrator, **quiero** ejecutar compensaciones automáticas ante un fallo de inventario o pago, **para** dejar siempre el sistema en un estado terminal consistente. |
| HU-05 | **Como** operador/SRE, **quiero** que un mismo `traceId` atraviese todos los servicios, **para** diagnosticar cualquier orden extremo a extremo en una sola traza. |
| HU-06 | **Como** operador/SRE, **quiero** que los mensajes irrecuperables vayan a una DLQ con la causa en headers, **para** inspeccionarlos y reprocesarlos sin perder eventos. |
| HU-07 | **Como** plataforma, **quiero** procesar reenvíos duplicados de forma idempotente, **para** garantizar at-least-once sin efectos secundarios duplicados. |
| HU-08 | **Como** cliente que reintenta `POST /orders` con la misma `Idempotency-Key`, **quiero** obtener la misma orden, **para** no crear órdenes duplicadas por reintentos de red. |

### 2.5 Requisitos funcionales

#### Ciclo de vida de la orden y API REST

| ID | Requisito (verificable) |
|---|---|
| **RF-01** | El sistema DEBE exponer `POST /orders` que cree una `Order` en estado `PENDING` y, en la misma transacción de base de datos, persista el evento `OrderCreated` en la tabla outbox. |
| **RF-02** | `POST /orders` DEBE validar el payload y responder `400 Bad Request` sin persistir nada cuando sea inválido, o `201 Created` con `{ orderId, status }` cuando sea válido. |
| **RF-03** | `POST /orders` DEBE aceptar una cabecera `Idempotency-Key`; ante una clave ya vista, DEBE devolver la orden existente (mismo `orderId`) sin crear un duplicado. |
| **RF-04** | El sistema DEBE exponer `GET /orders/{id}` que devuelva el estado actual (`PENDING`/`CONFIRMED`/`CANCELLED`) y el historial de pasos de la saga, y `404 Not Found` si el `orderId` no existe. |
| **RF-05** | El estado de la `Order` SOLO puede transicionar según la máquina: `PENDING → CONFIRMED`, `PENDING → CANCELLED`; cualquier otra transición DEBE rechazarse. |

#### Orquestación de la saga

| ID | Requisito (verificable) |
|---|---|
| **RF-06** | El relay de outbox DEBE publicar `OrderCreated` en `order.events.v1` de forma confiable (poll-publisher) tras el commit, con garantía at-least-once. |
| **RF-07** | El Orchestrator DEBE emitir el comando `ReserveInventory` (a `inventory.commands.v1`) al observar `OrderCreated`. |
| **RF-08** | El Orchestrator DEBE emitir `ProcessPayment` (a `payment.commands.v1`) al recibir `InventoryReserved`. |
| **RF-09** | El Orchestrator DEBE ejecutar `ConfirmOrder` al recibir `PaymentAuthorized`, llevando la orden a `CONFIRMED` y emitiendo `OrderConfirmed` vía outbox. |
| **RF-10** | El Orchestrator DEBE persistir el estado de avance de la saga por `orderId`, de modo que tras una caída se reanude sin repetir pasos ya completados. |
| **RF-11** | Todos los mensajes de una misma orden DEBEN usar `orderId` como key de Kafka, garantizando orden total por orden y paralelismo entre órdenes distintas. |

#### Compensaciones

| ID | Requisito (verificable) |
|---|---|
| **RF-12** | Ante `InventoryReservationFailed`, el Orchestrator DEBE marcar la orden `CANCELLED` y emitir `OrderCancelled`, sin invocar al pago. |
| **RF-13** | Ante `PaymentDeclined`, el Orchestrator DEBE iniciar la compensación emitiendo `ReleaseInventory` antes de cancelar la orden. |
| **RF-14** | Al recibir `InventoryReleased` (resultado de la compensación), el Orchestrator DEBE marcar la orden `CANCELLED` y emitir `OrderCancelled`. |
| **RF-15** | Si tras una `Order` confirmada se requiriera revertir un cobro, el sistema DEBE soportar `RefundPayment` produciendo `PaymentRefunded` (reembolso idempotente). |
| **RF-16** | Toda saga DEBE terminar en exactamente un estado terminal (`CONFIRMED` o `CANCELLED`); no se permiten sagas colgadas. |

#### Idempotencia observable y entrega

| ID | Requisito (verificable) |
|---|---|
| **RF-17** | Cada consumidor DEBE ser idempotente usando la clave `(consumerName, messageId)`, registrando el procesamiento en la tabla `processed_messages` y/o Redis. |
| **RF-18** | El reprocesamiento de un mensaje ya procesado (reentrega at-least-once) NO DEBE producir efectos secundarios adicionales (ni doble reserva, ni doble cobro, ni doble notificación visible). |
| **RF-19** | La idempotencia DEBE ser observable: cada deduplicación DEBE incrementar una métrica (p.ej. `messages_deduplicated_total`) y registrarse en log estructurado con `messageId` y `consumerName`. |
| **RF-20** | El sistema DEBE operar con semántica at-least-once + idempotencia (no exactly-once nativo) y garantizar cero pérdida de eventos vía outbox. |

#### Manejo de DLQ y errores

| ID | Requisito (verificable) |
|---|---|
| **RF-21** | Cada consumidor DEBE reintentar el procesamiento con backoff exponencial; tras agotar N reintentos, el mensaje DEBE enrutarse al topic `<topic>.dlq`. |
| **RF-22** | El mensaje enviado a la DLQ DEBE incluir headers con la causa del fallo (excepción, stacktrace resumido, topic/partición/offset original y `messageId`). |
| **RF-23** | El operador DEBE poder inspeccionar la DLQ (vía AKHQ) y reprocesar manualmente un mensaje sin que ello rompa la idempotencia (no duplica efectos). |
| **RF-24** | Un fallo transitorio (p.ej. timeout del gateway simulado) NO DEBE enviar el mensaje a la DLQ antes de agotar la política de reintentos configurada. |

#### Notificación y observabilidad funcional

| ID | Requisito (verificable) |
|---|---|
| **RF-25** | `notification-service` DEBE consumir `OrderConfirmed` y `OrderCancelled` y emitir `NotificationSent` de forma idempotente (una notificación efectiva por evento terminal). |
| **RF-26** | Cada paso de la saga DEBE estar trazado con un `traceId` propagado extremo a extremo entre todos los servicios, visible en la herramienta de trazas. |
| **RF-27** | Cada orden DEBE exponer un historial auditable de su saga (eventos y comandos con timestamp) consultable mediante `GET /orders/{id}` y `GET /orders/{id}/events` ([§5.6.3](#563-get-apiv1ordersidevents--línea-de-tiempo-de-la-saga)). |

---

## 3. Requisitos no funcionales

Esta sección define los requisitos no funcionales (RNF) de **OrderFlow**. Cada RNF incluye una métrica objetivo verificable y el mecanismo de verificación. Las métricas marcadas como *(borrador)* son objetivos iniciales ajustables tras la primera línea base de carga. Los métodos de verificación abreviados se detallan en [§3.9](#39-métodos-de-verificación-referencia).

### 3.1 Rendimiento

| ID | Requisito | Métrica objetivo | Cómo se verifica |
|---|---|---|---|
| RNF-01 | Throughput sostenido de creación y procesamiento de órdenes en hardware de laptop | >= 1.000 órdenes/min sostenidas durante >= 10 min sin crecimiento monótono del *consumer lag* | Prueba de carga k6/Gatling contra `POST /orders`; se observa `kafka_consumergroup_lag` en Grafana y se confirma lag estable o decreciente |
| RNF-02 | Latencia extremo a extremo de completar la saga (PENDING -> CONFIRMED/CANCELLED), excluyendo latencia simulada del gateway de pago | p95 < 1s y p99 < 2s *(borrador)* | Histograma Micrometer `saga_duration_seconds` (timer desde `OrderCreated` hasta evento terminal); percentiles en Prometheus/Grafana durante la prueba de carga |
| RNF-03 | Latencia de la API REST de creación de orden (solo persistencia + outbox, respuesta `201`) | p99 < 150 ms bajo carga nominal | Métrica `http_server_requests_seconds` (Micrometer) filtrada por `uri=/orders,method=POST` |
| RNF-04 | Retardo del *outbox relay* (tiempo entre commit del evento en outbox y su publicación en Kafka) | p99 < 500 ms con intervalo de poll <= 200 ms | Timer `outbox_publish_delay_seconds` (diferencia entre `created_at` del registro y timestamp de publicación) |
| RNF-05 | Eficiencia de recursos en reposo y bajo carga nominal (todos los servicios en una laptop) | Sin OOM; uso estable de heap (sin fugas) durante 30 min de carga | Métricas `jvm_memory_used_bytes` y `process_cpu_usage` en Grafana; prueba de soak de 30 min |

### 3.2 Escalabilidad

| ID | Requisito | Métrica objetivo | Cómo se verifica |
|---|---|---|---|
| RNF-06 | Paralelismo entre órdenes preservando orden total por orden (key = `orderId`) | 6 particiones por topic; orden estricto por `orderId` garantizado | Inspección de config de topics (AKHQ); test de integración que verifica secuencia de eventos por orden bajo concurrencia |
| RNF-07 | Escalado horizontal de consumidores sin reconfiguración manual | Hasta 6 instancias activas por *consumer group*; el rebalanceo redistribuye particiones sin pérdida ni duplicado lógico | Levantar N réplicas de un servicio; verificar asignación de particiones en AKHQ y ausencia de eventos perdidos/duplicados procesados |
| RNF-08 | Escalado independiente por *bounded context* (cada servicio escala por separado) | Cada servicio es un proceso/contenedor autónomo con su propio *consumer group* y BD | Revisión de `docker-compose.yml` y configuración de `group.id` por servicio |
| RNF-09 | Degradación controlada ante picos (back-pressure) | Sin pérdida de mensajes ante picos 2x el throughput objetivo; absorción vía lag temporal y recuperación a estado estable | Prueba de carga con pico transitorio; verificar drenaje del lag y consistencia final |

### 3.3 Fiabilidad y consistencia

| ID | Requisito | Métrica objetivo | Cómo se verifica |
|---|---|---|---|
| RNF-10 | Cero pérdida de eventos (escritura atómica estado + evento) | 0 eventos perdidos: todo cambio de estado tiene su evento en outbox en la misma transacción | Test de integración (Testcontainers): inyectar fallo tras commit y antes de publicar; verificar publicación posterior por el relay. Reconciliación: `count(outbox)` == `count(publicados)` |
| RNF-11 | Semántica de entrega at-least-once con consumidores idempotentes | 0 efectos secundarios duplicados ante reentrega; dedup por clave `(consumerName, messageId)` | Test que reenvía el mismo mensaje N veces y verifica un único efecto (estado y eventos emitidos). Métrica `messages_deduplicated_total` > 0 en el escenario |
| RNF-12 | Recuperación sin efectos duplicados ante caída de un consumidor a mitad de saga | La saga se reanuda y converge al mismo estado terminal; sin doble cobro ni doble reserva | Test de caos: matar instancia tras consumir y antes de hacer commit del offset; reiniciar; verificar idempotencia (tabla `processed_messages`) y estado final correcto |
| RNF-13 | Compensaciones automáticas (transacciones compensatorias de la saga) | 100% de las sagas fallidas dejan el sistema en estado consistente (stock liberado / pago reembolsado / `Order(CANCELLED)`) | Tests por rama de fallo: `InventoryReservationFailed`, `PaymentDeclined`; verificar `ReleaseInventory`/`RefundPayment` y estado `CANCELLED` |
| RNF-14 | Consistencia eventual acotada del estado de la orden | Estado terminal alcanzado para >= 99,9% de órdenes en < 5s (sin inyección de fallos) | Consulta de reconciliación periódica: órdenes en estado no terminal con antigüedad > 5s == 0 bajo operación normal |
| RNF-15 | Manejo de eventos fuera de orden / tardíos | Transiciones de estado idempotentes y validadas por máquina de estados; eventos inválidos para el estado actual se descartan o enrutan a DLQ | Test que entrega eventos en orden inesperado; verificar que no hay transiciones ilegales |

### 3.4 Disponibilidad y resiliencia

| ID | Requisito | Métrica objetivo | Cómo se verifica |
|---|---|---|---|
| RNF-16 | Circuit breaker en llamadas a dependencias externas (gateway de pago simulado) | Apertura del *breaker* tras >= 50% de fallos en ventana de 20 llamadas; transición a *half-open* en <= 5s | Test con Resilience4j: forzar fallos y observar estados via `resilience4j_circuitbreaker_state` (Micrometer) |
| RNF-17 | Reintentos con backoff exponencial ante fallos transitorios | N reintentos (p. ej. 3) con backoff exponencial + *jitter*; sin tormenta de reintentos | Inspección de config `DefaultErrorHandler`/Resilience4j; test que cuenta intentos y verifica espaciado creciente |
| RNF-18 | Dead Letter Queue tras agotar reintentos | 100% de mensajes irrecuperables van a `<topic>.dlq` con headers de causa (excepción, stacktrace, offset original) | Test que fuerza fallo permanente; verificar presencia del mensaje en `.dlq` y headers (`DeadLetterPublishingRecoverer`). Alerta sobre `dlq_messages_total > 0` |
| RNF-19 | Graceful shutdown sin pérdida de trabajo en vuelo | Drenaje de consumidores y commit de offsets pendientes antes de cerrar; sin reprocesos por offsets no confirmados (mitigados por idempotencia) | `SIGTERM` a una instancia bajo carga; verificar cierre limpio en logs y ausencia de mensajes perdidos. `spring.lifecycle.timeout-per-shutdown-phase` configurado |
| RNF-20 | Aislamiento de fallos entre servicios | La caída de `notification-service` no impide completar la saga; la caída de `payment-service` solo bloquea el paso de cobro (recuperable al reanudar) | Test de caos: detener un servicio no crítico y verificar progreso de la saga; reiniciar y verificar drenaje del lag |
| RNF-21 | Tolerancia a indisponibilidad transitoria de infraestructura (Kafka/Postgres/Redis) | Reconexión automática; ninguna pérdida de datos confirmados tras restablecer la dependencia | Test de integración deteniendo el contenedor de la dependencia y restaurándolo; verificar continuidad y consistencia |

### 3.5 Observabilidad

| ID | Requisito | Métrica objetivo | Cómo se verifica |
|---|---|---|---|
| RNF-22 | Trazabilidad distribuida extremo a extremo de la saga | 100% de los pasos de la saga comparten un único `traceId` que atraviesa todos los servicios (propagado por headers Kafka) | Inspección en Tempo/Jaeger: una orden produce una traza única con spans de los 4 servicios; test que valida propagación del contexto OTel |
| RNF-23 | Métricas de negocio y técnicas expuestas a Prometheus | Endpoint `/actuator/prometheus` activo en todos los servicios; SLIs definidos (ver RNF-25) | Scrape de Prometheus operativo; dashboards Grafana con paneles de throughput, latencia de saga, lag, tasa de DLQ |
| RNF-24 | Logging estructurado en JSON con correlación | 100% de logs en JSON incluyen `traceId`, `spanId` y `orderId` (cuando aplique); 0 logs de texto plano en producción | Inspección de salida de logs; validación de esquema JSON en pipeline; búsqueda por `orderId`/`traceId` cruzando servicios |
| RNF-25 | SLIs definidos y monitorizados | SLIs: tasa de éxito de sagas (>= 99,9%), latencia p99 de saga (RNF-02), *consumer lag*, tasa de mensajes en DLQ (objetivo ~0) | Paneles dedicados en Grafana; reglas de alerta en Prometheus por umbral de cada SLI |
| RNF-26 | Métricas de salud y readiness | Liveness/readiness diferenciadas; *readiness* refleja conectividad a Kafka/BD | `/actuator/health` con grupos `liveness`/`readiness`; test que verifica `503` cuando una dependencia crítica está caída |

### 3.6 Seguridad

| ID | Requisito | Métrica objetivo | Cómo se verifica |
|---|---|---|---|
| RNF-27 | Gestión de secretos fuera del código | 0 credenciales hardcodeadas; secretos vía variables de entorno / `.env` no versionado (Vault como evolución) | Escaneo de secretos en CI (gitleaks/trufflehog); revisión de `.gitignore` y ausencia de credenciales en el repositorio |
| RNF-28 | Validación de entrada en la API REST | 100% de los endpoints validan payload (Bean Validation `@Valid`); entradas inválidas devuelven `400` con detalle | Tests de contrato/integración con payloads malformados; verificar `400` y cuerpo de error estructurado |
| RNF-29 | Sin PII en logs ni en trazas | 0 datos personales sensibles (PAN de tarjeta, email completo) en logs/trazas; enmascaramiento donde aplique | Revisión de patrones de logging; test que verifica ausencia/enmascaramiento de campos sensibles |
| RNF-30 | Autenticación básica en la API REST | Endpoints `/orders` protegidos (HTTP Basic / API key); endpoints de actuator sensibles restringidos | Test que verifica `401` sin credenciales y `200` con credenciales válidas |
| RNF-31 | Validación de esquema de mensajes (compatibilidad) | Compatibilidad de esquemas Avro forzada por Schema Registry (BACKWARD por defecto); mensajes no conformes rechazados | Configuración de compatibilidad en Schema Registry; test que intenta publicar con esquema incompatible y verifica rechazo |

### 3.7 Mantenibilidad y calidad de código

| ID | Requisito | Métrica objetivo | Cómo se verifica |
|---|---|---|---|
| RNF-32 | Cobertura de pruebas automatizadas | Cobertura de líneas >= 70% global; módulos de dominio/saga >= 85%; rutas críticas de la saga al 100% de ramas | Reporte JaCoCo en CI; *gate* que falla el build bajo el umbral |
| RNF-33 | Build reproducible e independiente del PATH | Compila con Java 25 fijado vía `maven-toolchains-plugin`, independientemente del JDK del PATH | `mvn verify` en CI con toolchain configurada; build verde en GitHub Actions |
| RNF-34 | Análisis estático y estilo de código | 0 *issues* bloqueantes de análisis estático; estilo consistente aplicado | Spotless/Checkstyle + análisis (SpotBugs/Sonar) en CI como *gate* |
| RNF-35 | Contract testing entre productores y consumidores | Contratos verificados para eventos/comandos clave; rotura de contrato falla el build | Spring Cloud Contract en CI; tests de contrato productor/consumidor |
| RNF-36 | Estructura modular y bajo acoplamiento | Multi-módulo Maven; lógica compartida en `common-lib` y esquemas en `events-schema`; sin dependencias cíclicas entre servicios | Revisión de grafo de dependencias del reactor Maven; ausencia de ciclos |
| RNF-37 | Trazabilidad de cambios y CI verde | Pipeline de CI (build + test + escaneo + build de imagen) obligatorio antes de merge | GitHub Actions configurado como *required check* en la rama principal |

### 3.8 Portabilidad y operabilidad

| ID | Requisito | Métrica objetivo | Cómo se verifica |
|---|---|---|---|
| RNF-38 | Arranque local reproducible sin pasos manuales | `docker compose up` + un único comando de build levantan todo el entorno (Kafka, Schema Registry, Postgres, Redis, AKHQ, Prometheus, Grafana) | Ejecución en entorno limpio; verificar que todos los servicios alcanzan estado *healthy* sin intervención |
| RNF-39 | Contenerización de todos los servicios | 100% de los servicios disponibles como imagen Docker construible reproduciblemente | Build de imágenes en CI; `docker compose up` con imágenes locales |
| RNF-40 | Inicialización automática de esquemas de BD y topics | Migraciones de BD (Flyway/Liquibase) y creación de topics automatizadas al arranque | Levantar en entorno limpio; verificar tablas (incluida `outbox`, `processed_messages`) y topics creados sin scripts manuales |
| RNF-41 | Configuración externalizada por entorno | 0 valores de entorno hardcodeados; toda config vía variables de entorno / perfiles Spring | Revisión de `application.yml` y `docker-compose.yml`; arranque con distinta config sin recompilar |
| RNF-42 | Portabilidad de plataforma | Arranque exitoso en Linux, macOS y Windows (Docker Desktop / WSL2) | Verificación de `docker compose up` en los tres sistemas operativos |

### 3.9 Métodos de verificación (referencia)

| Método | Descripción |
|---|---|
| **Prueba de carga** | k6 o Gatling generan tráfico contra `POST /orders`; métricas recolectadas en Prometheus y visualizadas en Grafana. |
| **Test de integración** | JUnit 5 + Testcontainers (Kafka, Postgres, Redis) + Awaitility para aserciones asíncronas. |
| **Test de caos** | Detención/reinicio controlado de instancias o contenedores de infraestructura para validar recuperación e idempotencia. |
| **Inspección de configuración** | Revisión de `docker-compose.yml`, `application.yml`, config de topics (AKHQ) y Schema Registry. |
| **Gate de CI** | Verificación automática en GitHub Actions (cobertura, análisis estático, contratos, escaneo de secretos) que bloquea el merge si falla. |
| **Observación en dashboards** | Validación de SLIs y métricas en Grafana, trazas en Tempo/Jaeger. |

> **Nota:** los valores marcados *(borrador)* (RNF-02 y umbrales asociados) se ratificarán tras establecer la primera línea base de rendimiento. Los objetivos de cobertura (RNF-32) aplican a partir del primer hito funcional de la saga.

---

## 4. Arquitectura del sistema

### 4.1. Vista de contexto (C4 nivel 1-2)

La siguiente vista combina el contexto (nivel 1) con la descomposición en contenedores (nivel 2). El cliente REST interactúa únicamente con `order-service`; el resto de la comunicación entre servicios es **asíncrona vía Kafka**. Cada servicio es dueño exclusivo de su base de datos Postgres (no hay BD compartida, ver [ADR-09](#adr-09-una-base-de-datos-por-servicio-database-per-service)) y todos comparten Redis para idempotencia y locks distribuidos.

```
                                  ┌──────────────────┐
                                  │   Cliente REST   │
                                  │ (curl / Postman) │
                                  └────────┬─────────┘
                                           │ HTTP/JSON  POST /orders, GET /orders/{id}
                                           ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                  PLATAFORMA OrderFlow                               │
│                                                                                    │
│   ┌───────────────────────────┐         ┌────────────────────────────┐            │
│   │      order-service        │         │     payment-service        │            │
│   │  REST + Saga Orchestrator │         │  Pago (gateway simulado)   │            │
│   │  ┌─────────────────────┐  │         │  ┌──────────────────────┐  │            │
│   │  │ Postgres: orderdb   │  │         │  │ Postgres: paymentdb  │  │            │
│   │  │  + outbox           │  │         │  │  + outbox            │  │            │
│   │  └─────────────────────┘  │         │  └──────────────────────┘  │            │
│   └─────────────┬─────────────┘         └─────────────┬──────────────┘            │
│                 │                                      │                           │
│   ┌─────────────┴─────────────┐         ┌─────────────┴──────────────┐            │
│   │    inventory-service      │         │    notification-service    │            │
│   │   Reserva/Libera stock    │         │   Notifica (logueado)      │            │
│   │  ┌─────────────────────┐  │         │  ┌──────────────────────┐  │            │
│   │  │ Postgres: inventory │  │         │  │ Postgres: notifydb   │  │            │
│   │  │  db + outbox        │  │         │  │  + outbox            │  │            │
│   │  └─────────────────────┘  │         │  └──────────────────────┘  │            │
│   └─────────────┬─────────────┘         └─────────────┬──────────────┘            │
│                 │                                      │                           │
│   ══════════════╪══════════════════════════════════════╪════════════════════     │
│                 ▼              APACHE KAFKA (KRaft)      ▼                         │
│   ┌────────────────────────────────────────────────────────────────────────┐     │
│   │  order.events  order.commands  payment.commands  payment.events         │     │
│   │  inventory.commands  inventory.events  notification.events  (+ *.dlq)    │     │
│   └────────────────────────────────────────────────────────────────────────┘     │
│                 │                                      │                           │
│                 ▼                                      ▼                           │
│   ┌──────────────────────────┐         ┌──────────────────────────────────┐       │
│   │  Confluent Schema         │        │   Redis 7                        │        │
│   │  Registry (Avro)          │        │  idempotencia / dedup / locks    │        │
│   └──────────────────────────┘         └──────────────────────────────────┘       │
│                                                                                    │
└──────────────────────────────────────────────────────────────────────────────────┘
            │ scrape /actuator/prometheus            │ traces OTLP
            ▼                                        ▼
   ┌─────────────────┐   ┌──────────┐      ┌──────────────────────┐
   │   Prometheus    │──▶│ Grafana  │      │  Tempo / Jaeger      │
   └─────────────────┘   └──────────┘      └──────────────────────┘
```

> Cada servicio publica eventos/comandos hacia Kafka **solo a través de su tabla `outbox`** (relay poll-publisher). Ningún servicio escribe directamente en la BD de otro. Schema Registry valida la compatibilidad de los esquemas Avro en producción y consumo.

### 4.2. Servicios y responsabilidades

| Servicio | Responsabilidad principal | Es dueño de | Produce | Consume |
|---|---|---|---|---|
| **order-service** | Agregado `Order` + **Saga Orchestrator**. API REST para crear/consultar órdenes. Coordina la saga y emite comandos; aplica transiciones de estado y compensaciones. | `orderdb`, agregado Order, máquina de estados de la saga | `OrderCreated`, `OrderConfirmed`, `OrderCancelled` (en `order.events.v1`); comandos `ReserveInventory`, `ReleaseInventory`, `ProcessPayment`, `RefundPayment` | `inventory.events.v1`, `payment.events.v1` |
| **inventory-service** | Reserva y libera stock de forma idempotente. Responde a comandos de inventario. | `inventorydb`, stock y reservas | `InventoryReserved`, `InventoryReleased`, `InventoryReservationFailed` (en `inventory.events.v1`) | `inventory.commands.v1` (`ReserveInventory`, `ReleaseInventory`) |
| **payment-service** | Procesa el cobro contra un gateway simulado, de forma idempotente. Autoriza, declina o reembolsa. | `paymentdb`, transacciones de pago | `PaymentAuthorized`, `PaymentDeclined`, `PaymentRefunded` (en `payment.events.v1`) | `payment.commands.v1` (`ProcessPayment`, `RefundPayment`) |
| **notification-service** | Consume eventos terminales de la orden y notifica (simulado/logueado). | `notifydb`, registro de notificaciones | `NotificationSent` (en `notification.events.v1`) | `order.events.v1` (`OrderConfirmed`, `OrderCancelled`) |

Módulos compartidos (no son servicios desplegables):

- **events-schema**: esquemas Avro (`.avsc`) y clases Java generadas. Única fuente de verdad de los contratos de mensajes.
- **common-lib**: relay del outbox, infraestructura de idempotencia (`processed_messages` + Redis), tracing (OpenTelemetry/Micrometer), manejo de errores y DLQ, configuración compartida de Kafka.

### 4.3. Topología de Kafka

Convención: todos los topics llevan sufijo `.v1`, **6 particiones**, `key = orderId` (garantiza orden total por orden y paralelismo entre órdenes, ver [ADR-06](#adr-06-particionamiento-por-orderid-para-orden)). Por cada topic **consumido** existe su `<topic>.dlq` (también 6 particiones, sin re-consumo automático).

| Topic | Tipo | Key | Part. | Productores | Consumidores (consumer group) | DLQ |
|---|---|---|---|---|---|---|
| `order.events.v1` | Evento | `orderId` | 6 | order-service | notification-service (`notification-order-events`) | `order.events.v1.dlq` |
| `order.commands.v1` | Comando | `orderId` | 6 | order-service _(comandos internos `ConfirmOrder`/`CancelOrder`; reservado para comandos externos a la orden)_ | order-service (`order-commands`) | `order.commands.v1.dlq` |
| `inventory.commands.v1` | Comando | `orderId` | 6 | order-service (orchestrator) | inventory-service (`inventory-commands`) | `inventory.commands.v1.dlq` |
| `inventory.events.v1` | Evento | `orderId` | 6 | inventory-service | order-service (`order-inventory-events`) | `inventory.events.v1.dlq` |
| `payment.commands.v1` | Comando | `orderId` | 6 | order-service (orchestrator) | payment-service (`payment-commands`) | `payment.commands.v1.dlq` |
| `payment.events.v1` | Evento | `orderId` | 6 | payment-service | order-service (`order-payment-events`) | `payment.events.v1.dlq` |
| `notification.events.v1` | Evento | `orderId` | 6 | notification-service | _(observabilidad / auditoría)_ | `notification.events.v1.dlq` |

> **Comandos vs. eventos**: los topics `*.commands.v1` transportan **intenciones** dirigidas (el orchestrator ordena); los `*.events.v1` transportan **hechos** ya ocurridos (broadcast). El orchestrator nunca consume sus propios comandos hacia otros servicios: consume los **eventos** de respuesta de inventory y payment para avanzar la saga. Los comandos internos `ConfirmOrder`/`CancelOrder` viajan por `order.commands.v1` y materializan transiciones internas del agregado `Order`.

### 4.4. Saga orquestada (comandos, eventos y compensaciones)

`order-service` actúa como orquestador centralizado ([ADR-01](#adr-01-saga-orquestada-vs-coreografiada)). Las líneas `═══▶` son comandos (intención); `───▶` son eventos (hecho). Las compensaciones aparecen marcadas con `[C]`.

```
                          ┌───────────────────────────────────────┐
                          │   order-service  (SAGA ORCHESTRATOR)   │
                          │   estados: PENDING→…→CONFIRMED/CANCELLED│
                          └───────────────────────────────────────┘
                              │  ▲              │  ▲
            (1) ReserveInventory│  │            (4) ProcessPayment│  │
                              ═══▶│  │───            ═══▶│  │───
                                  │  │(3) InventoryReserved /         │  │(5) PaymentAuthorized /
                                  │  │    InventoryReservationFailed  │  │    PaymentDeclined
                                  ▼  │                                ▼  │
                  ┌───────────────────────────┐          ┌───────────────────────────┐
                  │     inventory-service     │          │      payment-service      │
                  └───────────────────────────┘          └───────────────────────────┘

CAMINO FELIZ
  POST /orders ─▶ Order(PENDING) + OrderCreated(outbox)
       │
       ├─(2) ═══ ReserveInventory ════════▶ inventory-service
       │                                         │
       │◀──── InventoryReserved ─────────────────┘
       │
       ├─(4) ═══ ProcessPayment ═══════════▶ payment-service
       │                                         │
       │◀──── PaymentAuthorized ─────────────────┘
       │
       └─(6) ConfirmOrder ─▶ Order(CONFIRMED) + OrderConfirmed ─▶ notification-service ─▶ NotificationSent


COMPENSACIONES
  [C1] Fallo de inventario:
       InventoryReservationFailed ─▶ Order(CANCELLED) + OrderCancelled ─▶ notification-service
       (no hay nada que compensar; aún no se reservó stock ni se cobró)

  [C2] Pago declinado (ya hay stock reservado):
       PaymentDeclined
            │
            ═══ ReleaseInventory ═══════════▶ inventory-service
                                                  │
            ◀──── InventoryReleased ───────────────┘
            │
            ▼
       Order(CANCELLED) + OrderCancelled ─▶ notification-service ─▶ NotificationSent

  [C3] (extensión) Reembolso si el pago se autorizó pero falla un paso posterior:
       RefundPayment ═══▶ payment-service ───▶ PaymentRefunded ─▶ (continúa compensación)
```

### 4.5. Flujo de una orden de extremo a extremo

1. **Creación**: el cliente envía `POST /orders`. `order-service` persiste `Order` en estado `PENDING` y, **en la misma transacción JPA**, inserta el evento `OrderCreated` en la tabla `outbox` (atomicidad estado+evento).
2. **Publicación confiable**: el **outbox relay** (poll-publisher) de `order-service` lee filas pendientes del outbox y publica `OrderCreated` en `order.events.v1` (key = `orderId`), marcándolas como publicadas. *(Alternativa de producción: CDC con Debezium leyendo el WAL de Postgres, ver [ADR-02](#adr-02-transactional-outbox-vs-publicación-directa).)*
3. **Comando de reserva**: el Orchestrator emite el comando `ReserveInventory` hacia `inventory.commands.v1`.
4. **Reserva de stock**: `inventory-service` consume el comando de forma **idempotente** (clave `(consumerName, messageId)` en `processed_messages`/Redis), intenta reservar stock y publica vía su outbox:
   - `InventoryReserved` → continúa la saga.
   - `InventoryReservationFailed` → ir al paso 8 (compensación C1).
5. **Comando de pago**: con `InventoryReserved`, el Orchestrator emite `ProcessPayment` hacia `payment.commands.v1`.
6. **Cobro**: `payment-service` consume idempotentemente, llama al gateway simulado y publica `PaymentAuthorized` o `PaymentDeclined`.
   - `PaymentDeclined` → ir al paso 9 (compensación C2).
7. **Confirmación (camino feliz)**: con `PaymentAuthorized`, el Orchestrator ejecuta `ConfirmOrder`, transiciona `Order` a `CONFIRMED` y emite `OrderConfirmed` en `order.events.v1`.
8. **Compensación C1 (fallo de inventario)**: el Orchestrator transiciona `Order` a `CANCELLED` y emite `OrderCancelled`. No hay efectos que revertir.
9. **Compensación C2 (pago declinado)**: el Orchestrator emite `ReleaseInventory`; `inventory-service` responde `InventoryReleased`; entonces transiciona `Order` a `CANCELLED` y emite `OrderCancelled`.
10. **Notificación**: `notification-service` consume el evento terminal (`OrderConfirmed` u `OrderCancelled`), notifica (logueado) y emite `NotificationSent` en `notification.events.v1`.
11. **Trazabilidad**: el mismo `traceId` (OpenTelemetry) se propaga vía headers de Kafka a través de todos los servicios; cada paso queda trazado extremo a extremo y las métricas se exponen en `/actuator/prometheus`.

### 4.6. Estructura de módulos del repositorio (Maven multi-módulo)

```
orderflow/
├── pom.xml                          # POM padre (BOM, versiones, plugins, toolchains)
├── .mvn/                            # wrapper + toolchains
├── toolchains.xml                   # fija JDK 25 (maven-toolchains-plugin)
├── docker-compose.yml               # infra local completa
├── .github/
│   └── workflows/
│       └── ci.yml                   # build, test, escaneo, build de imagen
│
├── common-lib/                      # módulo compartido (no desplegable)
│   ├── pom.xml
│   └── src/main/java/com/orderflow/common/
│       ├── outbox/                  # entidad OutboxEvent + relay poll-publisher
│       ├── idempotency/             # processed_messages + Redis dedup
│       ├── tracing/                 # OpenTelemetry / Micrometer config
│       ├── error/                   # DefaultErrorHandler, DLQ, DeadLetterPublishingRecoverer
│       └── kafka/                   # config común de productores/consumidores
│
├── events-schema/                   # contratos Avro (no desplegable)
│   ├── pom.xml                      # avro-maven-plugin (genera clases)
│   └── src/main/avro/
│       ├── order/                   # OrderCreated.avsc, OrderConfirmed.avsc, OrderCancelled.avsc
│       ├── inventory/               # InventoryReserved/Released/ReservationFailed + comandos
│       ├── payment/                 # PaymentAuthorized/Declined/Refunded + comandos
│       └── notification/            # NotificationSent.avsc
│
├── order-service/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/orderflow/order/
│       ├── api/                     # controllers REST (POST /orders, GET /orders/{id})
│       ├── domain/                  # agregado Order, estados
│       ├── saga/                    # Orchestrator + máquina de estados + compensaciones
│       ├── outbox/                  # relay específico
│       └── messaging/               # listeners de inventory.events / payment.events
│
├── inventory-service/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/orderflow/inventory/
│       ├── domain/                  # stock, reservas
│       ├── messaging/               # listener de inventory.commands
│       └── outbox/
│
├── payment-service/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/orderflow/payment/
│       ├── domain/                  # transacciones, gateway simulado
│       ├── messaging/               # listener de payment.commands
│       └── outbox/
│
└── notification-service/
    ├── pom.xml
    ├── Dockerfile
    └── src/main/java/com/orderflow/notification/
        ├── domain/                  # registro de notificaciones
        ├── messaging/               # listener de order.events
        └── outbox/
```

### 4.7. Vista de despliegue (Docker Compose)

Todos los servicios y la infraestructura arrancan con un único `docker compose up`. Los servicios de aplicación exponen el actuator de Spring para métricas/health.

| Contenedor | Imagen (referencia) | Puerto host:contenedor | Rol |
|---|---|---|---|
| `kafka` | `confluentinc/cp-kafka` (KRaft) | `9092:9092` | Broker Kafka (sin ZooKeeper) |
| `schema-registry` | `confluentinc/cp-schema-registry` | `8081:8081` | Registro de esquemas Avro |
| `akhq` | `tchiotludo/akhq` | `8080:8080` | UI de exploración de Kafka |
| `postgres-order` | `postgres:16` | `5432:5432` | BD `orderdb` |
| `postgres-inventory` | `postgres:16` | `5433:5432` | BD `inventorydb` |
| `postgres-payment` | `postgres:16` | `5434:5432` | BD `paymentdb` |
| `postgres-notification` | `postgres:16` | `5435:5432` | BD `notifydb` |
| `redis` | `redis:7` | `6379:6379` | Idempotencia / dedup / locks |
| `prometheus` | `prom/prometheus` | `9090:9090` | Scrape de métricas Micrometer |
| `grafana` | `grafana/grafana` | `3000:3000` | Dashboards |
| `tempo` | `grafana/tempo` (o `jaegertracing/all-in-one`) | `3200:3200` (Jaeger UI `16686`) | Backend de trazas OTLP |
| `order-service` | build local | `8090:8090` | REST + Saga Orchestrator |
| `inventory-service` | build local | `8091:8091` | Reserva/libera stock |
| `payment-service` | build local | `8092:8092` | Procesa pagos |
| `notification-service` | build local | `8093:8093` | Notificaciones |

```
docker compose up
        │
        ├─ infra:   kafka ─ schema-registry ─ akhq
        │           postgres-{order,inventory,payment,notification} ─ redis
        │           prometheus ─ grafana ─ tempo/jaeger
        │
        └─ apps:    order-service(8090) ─ inventory-service(8091)
                    payment-service(8092) ─ notification-service(8093)

  Dependencias de arranque (depends_on + healthchecks):
    apps  ──▶ kafka, schema-registry, su propio postgres, redis
    akhq  ──▶ kafka, schema-registry
    grafana ──▶ prometheus, tempo
```

> Cada servicio de aplicación se conecta **únicamente** a su propia base Postgres, al broker Kafka, al Schema Registry y a Redis. Los puertos `543x` mapean a distintos contenedores Postgres para permitir inspección directa desde el host sin colisiones.

---

## 5. Modelo de dominio, contratos de eventos y APIs

Esta sección define los contratos de datos del sistema: el modelo de dominio por bounded context, el esquema relacional de soporte para los patrones Outbox e idempotencia, el catálogo canónico de eventos/comandos, los esquemas Avro, la estrategia de versionado en Schema Registry y los contratos REST de `order-service`.

### 5.1 Modelo de dominio

El modelo está distribuido por servicio (database-per-service, [ADR-09](#adr-09-una-base-de-datos-por-servicio-database-per-service)). No hay claves foráneas cruzadas entre bases de datos; la consistencia entre contextos es eventual y se logra vía eventos. Los agregados se identifican por UUID generados en el lado del productor (no autoincrementales), para evitar coordinación y permitir idempotencia.

#### 5.1.1 Agregado `Order` (order-service)

`Order` es el agregado raíz dueño de la Saga. Mantiene el estado de negocio y un estado interno de saga (`sagaStep`) que el orquestador usa para reanudar sin efectos duplicados tras una caída.

| Campo            | Tipo            | Notas                                                       |
|------------------|-----------------|-------------------------------------------------------------|
| `id`             | UUID (PK)       | `orderId`, también la **key** de Kafka.                     |
| `customerId`     | UUID            | Referencia lógica al cliente.                               |
| `status`         | enum            | `PENDING` \| `CONFIRMED` \| `CANCELLED`.                     |
| `sagaStep`       | enum            | `CREATED`, `INVENTORY_REQUESTED`, `INVENTORY_RESERVED`, `PAYMENT_REQUESTED`, `PAYMENT_AUTHORIZED`, `COMPENSATING`, `DONE`. |
| `totalAmount`    | NUMERIC(12,2)   | Suma de líneas; calculado en creación.                      |
| `currency`       | CHAR(3)         | ISO-4217 (p. ej. `USD`).                                     |
| `failureReason`  | VARCHAR(256)    | Causa de cancelación (declined, reservation_failed).        |
| `version`        | BIGINT          | Lock optimista JPA (`@Version`).                            |
| `createdAt`      | TIMESTAMPTZ     | Inmutable.                                                   |
| `updatedAt`      | TIMESTAMPTZ     | Última transición.                                          |

**Máquina de estados de negocio de `Order`** (estados de negocio en mayúsculas; los disparadores muestran el `sagaStep` interno avanzando en paralelo):

```
   POST /orders
   (OrderCreated)
        │
        ▼
   ┌─────────┐   InventoryReserved → ProcessPayment → PaymentAuthorized → ConfirmOrder   ┌───────────┐
   │ PENDING │ ──────────────────────────────────────────────────────────────────────▶ │ CONFIRMED │
   └─────────┘                                                                            └───────────┘
        │  │                                                                              (terminal)
        │  │  InventoryReservationFailed
        │  └─────────────────────────────────┐
        │                                     │
        │  PaymentDeclined                    │
        │  (compensa: ReleaseInventory →      │
        │             InventoryReleased)      │
        ▼                                     ▼
   ┌──────────────────────────────────────────────┐
   │                  CANCELLED                    │
   └──────────────────────────────────────────────┘
                    (terminal)
                  (OrderCancelled)
```

Transiciones permitidas (cualquier otra es rechazada por el orquestador y se considera un mensaje fuera de orden o duplicado, descartado por idempotencia):

| Desde     | Evento/Comando disparador      | Hacia       |
|-----------|--------------------------------|-------------|
| —         | `POST /orders`                 | `PENDING`   |
| `PENDING` | `InventoryReservationFailed`   | `CANCELLED` |
| `PENDING` | `PaymentDeclined` (+Release)   | `CANCELLED` |
| `PENDING` | `PaymentAuthorized` → Confirm  | `CONFIRMED` |
| `*`       | reintento del mismo evento     | sin cambio (idempotente) |

#### 5.1.2 Entidad `OrderItem` (order-service)

Entidad hija dentro del agregado `Order` (no es agregado raíz; se crea/modifica solo a través de `Order`).

| Campo        | Tipo          | Notas                                  |
|--------------|---------------|----------------------------------------|
| `id`         | UUID (PK)     |                                        |
| `orderId`    | UUID (FK)     | Pertenece a `Order` (misma BD).        |
| `sku`        | VARCHAR(64)   | Identificador de producto.             |
| `quantity`   | INT           | `> 0`.                                  |
| `unitPrice`  | NUMERIC(12,2) | Precio congelado al crear la orden.    |
| `lineTotal`  | NUMERIC(12,2) | `quantity * unitPrice`.                 |

#### 5.1.3 Agregado `Payment` (payment-service)

| Campo           | Tipo          | Notas                                                   |
|-----------------|---------------|---------------------------------------------------------|
| `id`            | UUID (PK)     | `paymentId`.                                            |
| `orderId`       | UUID          | Correlación con la orden (key de Kafka).               |
| `amount`        | NUMERIC(12,2) |                                                         |
| `currency`      | CHAR(3)       |                                                         |
| `status`        | enum          | `AUTHORIZED` \| `DECLINED` \| `REFUNDED`.               |
| `gatewayRef`    | VARCHAR(128)  | Referencia del gateway simulado.                        |
| `idempotencyKey`| VARCHAR(128)  | `ProcessPayment.commandId`; **único**, evita doble cobro. |
| `createdAt`     | TIMESTAMPTZ   |                                                         |

#### 5.1.4 Agregado `InventoryItem` (inventory-service)

La reserva se modela con stock disponible y reservado para soportar liberación compensatoria idempotente.

| Campo          | Tipo        | Notas                                         |
|----------------|-------------|-----------------------------------------------|
| `sku`          | VARCHAR(64) (PK) | Producto.                                 |
| `available`    | INT         | Stock libre (`>= 0`).                          |
| `reserved`     | INT         | Stock reservado pendiente de confirmar.        |
| `version`      | BIGINT      | Lock optimista para reservas concurrentes.     |

`Reservation` (tabla auxiliar) liga `orderId → sku → quantity → status(RESERVED/RELEASED)`, garantizando que `ReleaseInventory` solo libere lo efectivamente reservado y sea idempotente.

### 5.2 Esquemas de soporte: Outbox y processed_messages (DDL)

Ambas tablas viven en **cada** base de datos de servicio. `outbox` se escribe en la misma transacción que el cambio de estado del agregado (Transactional Outbox, [ADR-02](#adr-02-transactional-outbox-vs-publicación-directa)). `processed_messages` da idempotencia a los consumidores ([ADR-04](#adr-04-estrategia-de-idempotenciadeduplicación)).

```sql
-- =========================================================
-- Tabla OUTBOX (una por base de datos de servicio)
-- Escrita atómicamente junto al cambio de estado del agregado.
-- El relay (poll-publisher) la lee, publica en Kafka y marca published_at.
-- Alternativa de producción: CDC con Debezium leyendo el WAL en lugar del relay.
-- =========================================================
CREATE TABLE outbox (
    id              UUID         PRIMARY KEY,                 -- = eventId (header Kafka)
    aggregate_type  VARCHAR(64)  NOT NULL,                    -- 'Order' | 'Payment' | 'Inventory'
    aggregate_id    UUID         NOT NULL,                    -- = orderId (=> Kafka message key)
    event_type      VARCHAR(64)  NOT NULL,                    -- 'OrderCreated', 'ReserveInventory', ...
    topic           VARCHAR(128) NOT NULL,                    -- destino: 'order.events.v1', ...
    payload         BYTEA        NOT NULL,                    -- Avro serializado (o JSON en dev)
    headers         JSONB        NOT NULL DEFAULT '{}'::jsonb,-- traceId, correlationId, schemaVersion...
    schema_version  INT          NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,                              -- NULL => pendiente
    attempts        INT          NOT NULL DEFAULT 0
);

-- El relay barre lo no publicado en orden de creación, por partición lógica (aggregate_id).
CREATE INDEX idx_outbox_unpublished
    ON outbox (created_at)
    WHERE published_at IS NULL;

-- =========================================================
-- Tabla PROCESSED_MESSAGES (idempotencia de consumidores)
-- Clave de idempotencia = (consumer_name, message_id).
-- Se inserta en la MISMA transacción que el efecto del mensaje.
-- Redis se usa como caché/fast-path; Postgres es la fuente de verdad.
-- =========================================================
CREATE TABLE processed_messages (
    consumer_name   VARCHAR(128) NOT NULL,   -- 'order-orchestrator', 'payment-processor', ...
    message_id      UUID         NOT NULL,    -- = eventId del mensaje consumido
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    result_hash     VARCHAR(64),              -- opcional: hash del efecto, para auditoría
    PRIMARY KEY (consumer_name, message_id)
);
```

Flujo de idempotencia del consumidor: (1) `SELECT` o intento de `INSERT` en `processed_messages`; si ya existe → ACK y descartar; (2) si no existe → ejecutar efecto + `INSERT` outbox/estado + `INSERT processed_messages`, todo en una transacción; (3) ACK. Como la entrega es **at-least-once**, esto garantiza efectos exactamente-una-vez a nivel de aplicación.

### 5.3 Catálogo de eventos y comandos

Convenciones: todos los topics llevan sufijo `.v1`, 6 particiones y `key = orderId`. Los **eventos** son hechos en pasado; los **comandos** son intenciones (imperativo). Cada topic consumido tiene su `<topic>.dlq`.

#### 5.3.1 Eventos (hechos)

| Evento                       | Topic                  | Productor          | Propósito                                  | Campos clave                                  |
|------------------------------|------------------------|--------------------|--------------------------------------------|-----------------------------------------------|
| `OrderCreated`               | `order.events.v1`      | order-service      | Inicia la saga.                            | `orderId, customerId, items[], totalAmount`   |
| `OrderConfirmed`             | `order.events.v1`      | order-service      | Saga completada con éxito.                 | `orderId, confirmedAt`                         |
| `OrderCancelled`             | `order.events.v1`      | order-service      | Saga abortada/compensada.                  | `orderId, reason`                              |
| `InventoryReserved`          | `inventory.events.v1`  | inventory-service  | Stock reservado OK.                        | `orderId, sku, quantity, reservationId`        |
| `InventoryReleased`          | `inventory.events.v1`  | inventory-service  | Compensación de reserva.                   | `orderId, reservationId`                        |
| `InventoryReservationFailed` | `inventory.events.v1`  | inventory-service  | No hay stock → cancelar.                   | `orderId, sku, reason`                          |
| `PaymentAuthorized`          | `payment.events.v1`    | payment-service    | Cobro aprobado.                            | `orderId, paymentId, amount, gatewayRef`       |
| `PaymentDeclined`            | `payment.events.v1`    | payment-service    | Cobro rechazado → compensar inventario.    | `orderId, reason`                              |
| `PaymentRefunded`            | `payment.events.v1`    | payment-service    | Reverso de cobro (compensación tardía).    | `orderId, paymentId, amount`                   |
| `NotificationSent`           | `notification.events.v1`| notification-service | Confirmación de notificación.           | `orderId, channel, status`                     |

#### 5.3.2 Comandos (intenciones)

| Comando            | Topic                    | Emisor (orquestador) | Consumidor         | Propósito                          | Campos clave                          |
|--------------------|--------------------------|----------------------|--------------------|------------------------------------|---------------------------------------|
| `ReserveInventory` | `inventory.commands.v1`  | order-service        | inventory-service  | Reservar stock para la orden.      | `orderId, items[]`                    |
| `ReleaseInventory` | `inventory.commands.v1`  | order-service        | inventory-service  | Compensar reserva.                 | `orderId, reservationId`              |
| `ProcessPayment`   | `payment.commands.v1`    | order-service        | payment-service    | Cobrar la orden.                   | `orderId, amount, currency, commandId`|
| `RefundPayment`    | `payment.commands.v1`    | order-service        | payment-service    | Reembolsar (compensación).         | `orderId, paymentId`                  |
| `ConfirmOrder`     | `order.commands.v1`      | order-service        | order-service      | Transición interna a CONFIRMED.    | `orderId`                             |
| `CancelOrder`      | `order.commands.v1`      | order-service        | order-service      | Transición interna a CANCELLED.    | `orderId, reason`                     |

### 5.4 Esquemas Avro de ejemplo

Namespace canónico: `com.orderflow.events.v1`. Tipos lógicos: `uuid`, `decimal`, `timestamp-millis`. Los campos opcionales se modelan como uniones con `null` por defecto para preservar compatibilidad BACKWARD ([ADR-07](#adr-07-avro--schema-registry-vs-json)).

**5.4.1 `OrderCreated` (evento)**

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "com.orderflow.events.v1",
  "doc": "Hecho: una orden fue creada y la saga inicia.",
  "fields": [
    { "name": "eventId",    "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "orderId",    "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "customerId", "type": { "type": "string", "logicalType": "uuid" } },
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "OrderLine",
          "fields": [
            { "name": "sku",       "type": "string" },
            { "name": "quantity",  "type": "int" },
            { "name": "unitPrice", "type": { "type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2 } }
          ]
        }
      }
    },
    { "name": "totalAmount", "type": { "type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2 } },
    { "name": "currency",    "type": "string", "default": "USD" },
    { "name": "occurredAt",  "type": { "type": "long", "logicalType": "timestamp-millis" } }
  ]
}
```

**5.4.2 `ReserveInventory` (comando)**

```json
{
  "type": "record",
  "name": "ReserveInventory",
  "namespace": "com.orderflow.events.v1",
  "doc": "Intencion: reservar stock para los items de una orden.",
  "fields": [
    { "name": "commandId",  "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "orderId",    "type": { "type": "string", "logicalType": "uuid" } },
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "ReserveLine",
          "fields": [
            { "name": "sku",      "type": "string" },
            { "name": "quantity", "type": "int" }
          ]
        }
      }
    },
    { "name": "issuedAt",   "type": { "type": "long", "logicalType": "timestamp-millis" } }
  ]
}
```

**5.4.3 `PaymentAuthorized` (evento)**

```json
{
  "type": "record",
  "name": "PaymentAuthorized",
  "namespace": "com.orderflow.events.v1",
  "doc": "Hecho: el gateway autorizo el cobro de la orden.",
  "fields": [
    { "name": "eventId",    "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "orderId",    "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "paymentId",  "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "amount",     "type": { "type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2 } },
    { "name": "currency",   "type": "string", "default": "USD" },
    { "name": "gatewayRef", "type": "string" },
    { "name": "authCode",   "type": ["null", "string"], "default": null },
    { "name": "occurredAt", "type": { "type": "long", "logicalType": "timestamp-millis" } }
  ]
}
```

### 5.5 Versionado, compatibilidad y headers estándar

#### 5.5.1 Compatibilidad en Schema Registry

- **Modo de compatibilidad: `BACKWARD`** (por subject). Garantiza que un consumidor con el esquema **nuevo** puede leer datos producidos con el esquema **antiguo**. Es el modo idóneo cuando se actualizan **consumidores antes que productores** (orden de despliegue habitual en este sistema, donde los servicios consumidores se actualizan primero).
- Reglas prácticas que impone BACKWARD:
  - Añadir un campo **solo** con `default` (los registros viejos lo rellenan con el default).
  - Eliminar un campo es válido **solo si tenía** `default`.
  - **Prohibido** renombrar campos o cambiar tipos incompatibles → eso exige un **subject nuevo** (cambio mayor) y, en consecuencia, un **topic nuevo con sufijo de versión** (`order.events.v2`).
- **Naming strategy:** `TopicRecordNameStrategy`, de modo que un mismo topic (p. ej. `order.events.v1`) puede transportar múltiples tipos de record (`OrderCreated`, `OrderConfirmed`, `OrderCancelled`) cada uno con su propio subject y evolución independiente.
- El cambio de versión `.v1 → .v2` en el **topic** se reserva para rupturas no resolubles bajo BACKWARD; la evolución aditiva normal **no** cambia el sufijo del topic.

#### 5.5.2 Claves de mensaje (partition key)

`key = orderId` en **todos** los topics. Esto fuerza que todos los eventos/comandos de una misma orden caigan en la **misma partición** → orden total por orden, con paralelismo entre órdenes distintas. La key se serializa como `String` (UUID); el value como Avro vía Schema Registry.

#### 5.5.3 Headers estándar de Kafka

Todo mensaje (evento o comando) transporta estos headers, propagados por `common-lib` y rellenados desde el contexto de OpenTelemetry:

| Header           | Tipo   | Origen / Uso                                                        |
|------------------|--------|---------------------------------------------------------------------|
| `eventId`        | UUID   | Identidad del mensaje. = `outbox.id`. Clave de idempotencia/dedup.   |
| `traceId`        | String | W3C `traceparent` de OpenTelemetry. Traza E2E un mismo `traceId`.    |
| `correlationId`  | UUID   | = `orderId`. Correla todos los mensajes de una saga.                 |
| `schemaVersion`  | Int    | Versión del schema del payload (espejo de `outbox.schema_version`).  |
| `eventType`      | String | Nombre del record (`OrderCreated`...) para ruteo sin deserializar.   |
| `causationId`    | UUID   | `eventId` del mensaje que causó este (cadena de causalidad).         |

### 5.6 Contratos de la API REST de order-service

Base path: `/api/v1`. Formato: `application/json`. Errores con cuerpo estilo **RFC 7807 (Problem Details)**. Todos los endpoints aceptan/propagan `traceparent`.

#### 5.6.1 `POST /api/v1/orders` — Crear orden

Persiste `Order(PENDING)` + `OrderCreated` en outbox en una sola transacción y responde de inmediato (la saga continúa asíncrona). Acepta header opcional `Idempotency-Key` para deduplicar reintentos del cliente.

Request:

```json
{
  "customerId": "8b1f...uuid",
  "currency": "USD",
  "items": [
    { "sku": "SKU-1001", "quantity": 2, "unitPrice": 19.99 },
    { "sku": "SKU-2002", "quantity": 1, "unitPrice": 5.50 }
  ]
}
```

Response `201 Created` (`Location: /api/v1/orders/{id}`):

```json
{
  "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "PENDING",
  "totalAmount": 45.48,
  "currency": "USD",
  "createdAt": "2026-06-14T12:00:00Z"
}
```

| HTTP | Condición                                                        |
|------|------------------------------------------------------------------|
| 201  | Orden creada.                                                    |
| 400  | Payload inválido (sin items, `quantity <= 0`, currency no ISO).  |
| 409  | `Idempotency-Key` ya usada con otro payload (conflicto).         |
| 422  | Reglas de negocio (p. ej. SKU desconocido si se valida síncrono).|
| 500  | Error interno.                                                   |

#### 5.6.2 `GET /api/v1/orders/{id}` — Consultar orden

Response `200 OK`:

```json
{
  "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "status": "CONFIRMED",
  "sagaStep": "DONE",
  "customerId": "8b1f...uuid",
  "totalAmount": 45.48,
  "currency": "USD",
  "failureReason": null,
  "items": [
    { "sku": "SKU-1001", "quantity": 2, "unitPrice": 19.99, "lineTotal": 39.98 },
    { "sku": "SKU-2002", "quantity": 1, "unitPrice": 5.50,  "lineTotal": 5.50 }
  ],
  "createdAt": "2026-06-14T12:00:00Z",
  "updatedAt": "2026-06-14T12:00:01Z"
}
```

| HTTP | Condición                  |
|------|----------------------------|
| 200  | Orden encontrada.          |
| 404  | `orderId` inexistente.     |

#### 5.6.3 `GET /api/v1/orders/{id}/events` — Línea de tiempo de la saga

Devuelve, en orden cronológico, los eventos/comandos de la saga para esa orden (proyección leída desde el historial/outbox del order-service). Útil para trazabilidad y depuración.

Response `200 OK`:

```json
{
  "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "traceId": "0af7651916cd43dd8448eb211c80319c",
  "events": [
    { "eventType": "OrderCreated",      "occurredAt": "2026-06-14T12:00:00.010Z", "eventId": "..." },
    { "eventType": "InventoryReserved", "occurredAt": "2026-06-14T12:00:00.230Z", "eventId": "..." },
    { "eventType": "PaymentAuthorized", "occurredAt": "2026-06-14T12:00:00.560Z", "eventId": "..." },
    { "eventType": "OrderConfirmed",    "occurredAt": "2026-06-14T12:00:01.000Z", "eventId": "..." }
  ]
}
```

| HTTP | Condición                       |
|------|---------------------------------|
| 200  | Historial devuelto (puede ir vacío si la saga apenas inicia). |
| 404  | `orderId` inexistente.          |

#### 5.6.4 Formato de error (RFC 7807)

```json
{
  "type": "https://orderflow.dev/errors/validation",
  "title": "Invalid order payload",
  "status": 400,
  "detail": "items must contain at least one line with quantity > 0",
  "instance": "/api/v1/orders",
  "traceId": "0af7651916cd43dd8448eb211c80319c",
  "errors": [
    { "field": "items[0].quantity", "message": "must be greater than 0" }
  ]
}
```

---

## 6. Decisiones de arquitectura (ADRs)

Esta sección registra las decisiones arquitectónicas significativas de OrderFlow en formato ADR (Architecture Decision Record). Cada ADR es inmutable una vez aceptada; cambios futuros se modelan como ADRs nuevas que la superseden. Todas las ADRs aquí listadas están en estado **Accepted** para la versión 0.1.0 del SDD.

> Convención: ADR-NN | Estado | Contexto | Decisión | Consecuencias (+/-) | Alternativas consideradas.

| ADR | Título | Estado |
|-----|--------|--------|
| ADR-01 | Saga orquestada vs coreografiada | Accepted |
| ADR-02 | Transactional Outbox vs publicación directa | Accepted |
| ADR-03 | At-least-once + idempotencia vs exactly-once | Accepted |
| ADR-04 | Estrategia de idempotencia/deduplicación | Accepted |
| ADR-05 | DLQ + retry con backoff exponencial | Accepted |
| ADR-06 | Particionamiento por orderId para orden | Accepted |
| ADR-07 | Avro + Schema Registry vs JSON | Accepted |
| ADR-08 | Java 25 (LTS) + Virtual Threads y Maven toolchains | Accepted |
| ADR-09 | Una base de datos por servicio (database-per-service) | Accepted |

### ADR-01: Saga orquestada vs coreografiada

**Estado:** Accepted

**Contexto.** El ciclo de vida de una orden cruza cuatro bounded contexts (order, inventory, payment, notification) y requiere transacciones compensatorias deterministas ante fallos (declinación de pago, falta de stock). Sin transacciones distribuidas ACID, necesitamos un patrón Saga. Las dos variantes son: coreografía (cada servicio reacciona a eventos de otros, sin coordinador central) u orquestación (un coordinador central dicta los pasos vía comandos).

**Decisión.** Implementar una **Saga orquestada** con el orquestador residiendo en `order-service`, dueño del agregado `Order`. El orquestador consume eventos de dominio (`InventoryReserved`, `PaymentAuthorized`, etc.) y emite comandos (`ReserveInventory`, `ProcessPayment`, `ReleaseInventory`, ...) que materializan la máquina de estados de la saga. El estado de la saga se persiste junto al agregado `Order`.

**Consecuencias**

*Positivas*
- La lógica del flujo y las compensaciones vive en un único lugar legible y testeable como máquina de estados explícita.
- Trazabilidad superior: el orquestador conoce el estado global de cada saga, facilitando consultas y observabilidad.
- Añadir un paso (p. ej. fraude) o cambiar el orden de compensaciones no requiere tocar todos los servicios, solo el orquestador.
- Separación limpia entre **comandos** (intenciones) y **eventos** (hechos), reflejada en topics distintos.

*Negativas*
- El `order-service` se vuelve un punto de acoplamiento lógico y un posible cuello de botella/SPOF si no se escala; se mitiga con consumidores particionados y estado en BD.
- Riesgo de "orquestador-dios" que acumule lógica de negocio ajena; se contiene limitando el orquestador a coordinación, no a reglas de dominio de otros contextos.
- Más comandos explícitos en el bus que en coreografía.

**Alternativas consideradas**
- *Coreografía pura (event-driven sin coordinador):* menor acoplamiento de despliegue, pero la lógica de la saga queda esparcida e implícita entre servicios, las compensaciones son difíciles de razonar y la trazabilidad del estado global exige reconstruirlo a partir de eventos. Descartada por complejidad operativa y de depuración para un sistema con compensaciones multi-paso.
- *Transacciones distribuidas (2PC/XA):* descartada de plano por acoplamiento síncrono, bloqueo de recursos y mala tolerancia a fallos en entorno asíncrono con Kafka.

### ADR-02: Transactional Outbox vs publicación directa

**Estado:** Accepted

**Contexto.** Cada servicio debe (a) cambiar su estado en PostgreSQL y (b) publicar un evento/comando en Kafka, de forma atómica. Escribir en la BD y publicar en Kafka son dos sistemas distintos sin transacción compartida (no hay XA fiable y eficiente entre JPA y Kafka). La publicación directa tras el commit abre una ventana de fallo: si el proceso muere entre el commit y el `producer.send()`, se pierde el evento (estado cambiado pero nadie notificado) — violando el objetivo de **cero pérdida de eventos**.

**Decisión.** Adoptar el patrón **Transactional Outbox**: en la misma transacción JPA que muta el agregado, insertar una fila en la tabla `outbox`. Un **relay tipo poll-publisher** (en `common-lib`) lee filas no publicadas, las envía a Kafka y las marca como publicadas. La publicación es **at-least-once** (un crash tras enviar pero antes de marcar reenvía el mensaje), lo cual es compatible con [ADR-03](#adr-03-semántica-at-least-once--consumidores-idempotentes-vs-exactly-once).

```
   POST /orders
        |
   [TX BEGIN]
     INSERT Order(PENDING)
     INSERT outbox(OrderCreated)   <-- misma TX, atomico
   [TX COMMIT]
        |
   (async) Outbox Relay  --poll-->  outbox  --send-->  Kafka(order.events.v1)
                                       |
                                  marca published=true
```

**Consecuencias**

*Positivas*
- Atomicidad real entre cambio de estado y registro del evento: sin eventos fantasma ni eventos perdidos.
- Desacopla la disponibilidad de Kafka de la transacción de negocio: si Kafka está caído, la orden se persiste y el relay reintenta después.
- La tabla outbox sirve como log de auditoría y permite reproceso.

*Negativas*
- Latencia adicional por el intervalo de polling (mitigable con polling agresivo o notificación); se acepta dentro del presupuesto de p99 < 2s.
- Carga de escritura/lectura extra en PostgreSQL y necesidad de limpiar (purgar) filas publicadas.
- Posibilidad de duplicados en publicación (at-least-once), que obliga a consumidores idempotentes ([ADR-04](#adr-04-estrategia-de-idempotenciadeduplicación)).
- El relay necesita orden y particionamiento correcto al publicar (key = orderId).

**Alternativas consideradas**
- *Publicación directa post-commit:* simple, pero con ventana de pérdida de eventos ante crash. Descartada por violar cero-pérdida.
- *CDC / Debezium leyendo el WAL de Postgres:* elimina el polling y reduce latencia, es el camino "productivo" recomendado a escala. Se documenta como **evolución futura**; se descarta para 0.1.0 por mayor complejidad operativa (conectores Kafka Connect, gestión de offsets del WAL) frente al valor didáctico del relay explícito.
- *Listen/Notify de Postgres para despertar el relay:* optimización compatible con la decisión; queda como mejora opcional sobre el poll-publisher.

### ADR-03: Semántica at-least-once + consumidores idempotentes vs exactly-once

**Estado:** Accepted

**Contexto.** En un pipeline Kafka con outbox, la entrega exactamente-una-vez (exactly-once) extremo a extremo no es gratuita ni completa: el patrón outbox + relay ya introduce at-least-once en la publicación, y los reintentos de consumidor reentregan mensajes. Kafka ofrece EOS (transacciones + `processing.guarantee=exactly_once_v2`) pero solo cubre el tramo consume-process-produce **dentro de Kafka**, no los efectos secundarios externos (escrituras a Postgres, llamadas al gateway de pago).

**Decisión.** Adoptar **at-least-once + consumidores idempotentes**. Cada mensaje puede entregarse más de una vez; la idempotencia en el consumidor ([ADR-04](#adr-04-estrategia-de-idempotenciadeduplicación)) garantiza que el efecto neto sea exactamente uno. No se usan transacciones EOS de Kafka.

**Consecuencias**

*Positivas*
- Modelo mental simple y robusto: "asume duplicados, diseña para tolerarlos".
- La idempotencia protege también contra reentregas por rebalanceo, reinicio de consumidor y reenvío del outbox — casos que EOS no cubre cuando hay efectos externos.
- Menor complejidad operativa y mejor rendimiento que las transacciones EOS (sin overhead de coordinación transaccional del broker).

*Negativas*
- Cada consumidor con efectos secundarios **debe** implementar idempotencia/dedup; es una obligación no negociable, no opcional.
- Ventana de duplicados visible si la dedup falla o expira (TTL en Redis); requiere dimensionar bien el almacén de dedup.

**Alternativas consideradas**
- *Exactly-once nativo (EOS v2):* atractivo sobre el papel, pero no abarca los efectos externos (pago, BD de negocio), añade overhead y complejidad de configuración, y sigue requiriendo idempotencia para las fronteras no-Kafka. Descartado por no resolver el problema real de extremo a extremo.
- *At-most-once:* inaceptable, implica pérdida de mensajes y viola cero-pérdida.

### ADR-04: Estrategia de idempotencia/deduplicación

**Estado:** Accepted

**Contexto.** [ADR-03](#adr-03-semántica-at-least-once--consumidores-idempotentes-vs-exactly-once) hace de la idempotencia un requisito de primer orden. Necesitamos una clave de deduplicación estable y un almacén donde registrar "ya procesado", funcionando bajo concurrencia (varias particiones/instancias) y reentregas.

**Decisión.** Clave de idempotencia compuesta = **(`consumerName`, `messageId`)**, donde `messageId` es un UUID generado por el productor y propagado en el evento/comando y en headers Kafka. La dedup se implementa en dos niveles:

1. **Capa rápida (Redis):** `SET key NX EX <ttl>`. Un `NX` que falla indica duplicado y el mensaje se descarta (ack) sin reprocesar. Provee dedup de baja latencia y locks distribuidos.
2. **Capa durable (PostgreSQL):** tabla `processed_messages(consumer_name, message_id, processed_at, PRIMARY KEY(consumer_name, message_id))`. La inserción del registro de dedup se realiza **en la misma transacción** que el efecto de negocio; un conflicto de PK confirma duplicado de forma autoritativa. Esto cierra la ventana cuando el TTL de Redis expira o Redis no está disponible.

```
mensaje --> Redis SETNX --hit?--> descartar (duplicado)
                |miss
                v
        [TX: efecto negocio + INSERT processed_messages]
                |  conflicto PK --> duplicado, rollback efecto
                v  commit --> ack
```

**Consecuencias**

*Positivas*
- Redis absorbe el grueso de duplicados con latencia mínima; Postgres garantiza corrección incluso si Redis falla o expira (Redis es optimización, no fuente de verdad).
- La unicidad por `(consumerName, messageId)` permite que distintos consumidores procesen el mismo mensaje sin interferir.
- Acoplar el INSERT de dedup a la transacción de negocio da idempotencia transaccional real (todo-o-nada).

*Negativas*
- Doble almacén (Redis + Postgres) añade infra y un punto de configuración de TTL; un TTL mal dimensionado puede dejar pasar duplicados si Postgres fuese la única red de seguridad.
- Crecimiento de `processed_messages`: requiere política de retención/particionado por tiempo.
- El productor debe garantizar un `messageId` estable por evento lógico (reenvíos del outbox deben conservar el mismo `messageId`).

**Alternativas consideradas**
- *Solo Redis:* rápido pero volátil; perder Redis o expirar el TTL reabre la ventana de duplicados. Insuficiente como única garantía.
- *Solo Postgres:* correcto y durable, pero añade carga y latencia a cada mensaje sin el filtro previo barato.
- *Idempotencia natural por estado (upsert/condición de estado):* útil y se aprovecha donde aplica (p. ej. transiciones de estado idempotentes en la saga), pero no generaliza a todos los efectos (cobro en gateway), por lo que se complementa con la clave de dedup.

### ADR-05: DLQ + retry con backoff exponencial

**Estado:** Accepted

**Contexto.** Un consumidor puede fallar por causas transitorias (BD momentáneamente no disponible, timeout del gateway) o permanentes (mensaje corrupto, regla de negocio imposible). Reintentar indefinidamente bloquea la partición (recordar: orden por orderId, [ADR-06](#adr-06-particionamiento-por-orderid-para-orden)) y propaga el fallo; descartar sin más pierde el mensaje.

**Decisión.** Estrategia de reintentos por capas:
- **Reintentos in-process con backoff exponencial** para fallos transitorios, usando el `DefaultErrorHandler` de Spring Kafka combinado con políticas de **Resilience4j** (retry, circuit breaker, rate limiter) en las llamadas a recursos externos (gateway de pago, otras BDs).
- Tras agotar **N reintentos**, el `DeadLetterPublishingRecoverer` enruta el mensaje a `<topic>.dlq` con **headers de causa** (excepción, stacktrace resumido, offset/partición original, contador de intentos, timestamp).
- La DLQ se monitoriza y permite reproceso manual/automatizado tras corregir la causa (runbook en [§8.5](#85-operabilidad)).

**Consecuencias**

*Positivas*
- Fallos transitorios se absorben sin intervención; el backoff exponencial evita tormentas de reintentos.
- Un mensaje "veneno" no bloquea indefinidamente su partición: se aparta a la DLQ y el consumo avanza.
- Los headers de causa hacen la DLQ accionable (diagnóstico y reproceso dirigido).
- El circuit breaker protege al gateway/BD aguas abajo de presión durante incidentes.

*Negativas*
- Mover a la DLQ puede **romper el orden** dentro de una orden si un mensaje se aparta mientras los siguientes avanzan; se acepta y se documenta — la saga compensatoria y la idempotencia mitigan, pero el reproceso desde DLQ debe considerar el estado actual de la saga.
- Operar la DLQ (alertas, herramienta de reproceso, runbook) es trabajo adicional.
- Elegir N y el backoff (base, multiplicador, max) requiere ajuste empírico por tipo de error.

**Alternativas consideradas**
- *Reintentos infinitos / bloqueo de partición (no DLQ):* preserva orden estricto pero un mensaje veneno detiene el procesamiento de esa orden para siempre. Descartado por disponibilidad.
- *Retry topics escalonados (retry.5s, retry.30s, ...):* patrón válido para no bloquear la partición durante el backoff; más topics e infraestructura. Se considera evolución si el backoff in-process resulta insuficiente.
- *Descartar tras N sin DLQ:* pierde mensajes y trazabilidad de fallos. Descartado.

### ADR-06: Particionamiento por orderId para orden

**Estado:** Accepted

**Contexto.** La saga de una orden exige procesar sus eventos/comandos **en el orden correcto** (no confirmar antes de cobrar, no cobrar antes de reservar). Kafka solo garantiza orden **dentro de una partición**. A la vez, queremos paralelismo entre órdenes distintas para alcanzar >= 1.000 órdenes/min.

**Decisión.** Usar **`key = orderId`** en todos los topics de la saga, con **6 particiones**. Kafka enruta por hash de la key, de modo que todos los mensajes de una misma orden caen en la misma partición y se procesan en orden total, mientras órdenes distintas se distribuyen entre particiones y se procesan en paralelo.

**Consecuencias**

*Positivas*
- Orden total garantizado por orden (requisito de la saga) sin coordinación global.
- Paralelismo natural: hasta 6 consumidores activos por grupo procesando órdenes en paralelo.
- Modelo simple y alineado con el reparto de la máquina de estados.

*Negativas*
- El paralelismo máximo de un grupo de consumidores está acotado por el número de particiones (6); escalar más allá exige re-particionar (operación no trivial: cambia el mapeo key->partición para órdenes en vuelo).
- Posible **hot partition** si la distribución de orderIds no es uniforme (poco probable con UUID/hash uniforme, pero a vigilar).
- Mover un mensaje a la DLQ rompe el orden de esa orden (ver [ADR-05](#adr-05-dlq--retry-con-backoff-exponencial)).

**Alternativas consideradas**
- *Sin key (round-robin):* máximo paralelismo pero **sin garantía de orden**; inviable para la saga.
- *Key por customerId:* serializaría todas las órdenes de un cliente (orden más fuerte de la necesaria) y agravaría hot partitions. Descartado.
- *Una partición (orden global):* orden trivialmente garantizado pero sin paralelismo; incompatible con el objetivo de throughput.
- *Dimensionar particiones:* 6 es un punto de partida para laptop; se reevalúa con pruebas de carga (k6/Gatling).

### ADR-07: Avro + Schema Registry vs JSON

**Estado:** Accepted

**Contexto.** Los eventos/comandos son el contrato entre servicios desarrollados y desplegados de forma independiente. Necesitamos un formato que (a) imponga un esquema, (b) gestione la evolución de ese esquema sin romper consumidores y (c) sea compacto y eficiente para el throughput objetivo.

**Decisión.** Usar **Apache Avro** con **Confluent Schema Registry**. Los esquemas viven en el módulo `events-schema`, que genera las clases Java en build. Configurar compatibilidad **BACKWARD** (como mínimo) en el registry para que nuevos consumidores lean datos antiguos. Cada mensaje lleva el `schemaId` y el payload binario.

**Consecuencias**

*Positivas*
- Contrato fuerte y versionado, validado por el registry; rompe la build/registro si una evolución viola compatibilidad.
- Payload binario compacto y serialización rápida: mejor throughput y menor uso de red/disco que JSON textual.
- Clases generadas => tipado fuerte en productores/consumidores, menos errores de mapeo.
- Evolución de esquema gobernada (añadir campos opcionales, defaults) sin coordinar despliegues simultáneos.

*Negativas*
- Schema Registry es un componente de infraestructura adicional (disponibilidad, despliegue local en Docker Compose).
- Mensajes no legibles "a ojo" sin deserializar (mitigado con AKHQ y herramientas que consultan el registry).
- Acoplamiento al ecosistema Confluent (serializadores/deserializadores específicos).
- Curva de aprendizaje de reglas de compatibilidad y defaults de Avro.

**Alternativas consideradas**
- *JSON (plano o con JSON Schema):* legible y sin infra extra, pero verboso, sin enforcement fuerte por defecto, evolución frágil y mayor coste de serialización/ancho de banda. Descartado para el bus principal; útil en bordes REST.
- *Protobuf:* alternativa sólida y compacta, también soportada por Schema Registry. Avro se elige por su integración idiomática con Kafka/Confluent y por el manejo de esquemas centrado en datos; Protobuf queda como opción equivalente válida.

### ADR-08: Java 25 (LTS) + Virtual Threads y Maven toolchains

**Estado:** Accepted (supersede la decisión de Java 21 de la v0.1.0)

**Contexto.** El sistema es de I/O intensivo (llamadas a BD, Kafka, gateway de pago simulado con latencia). El modelo clásico de un hilo de plataforma por petición limita la concurrencia y consume memoria. Además, el build debe compilar de forma reproducible con una versión de Java fija, independientemente del JDK que tenga el desarrollador en el `PATH` (el entorno de desarrollo convive con múltiples JDKs: 8/17/21/25).

**Decisión.**
- Adoptar **Java 25 (LTS)** — el LTS más reciente y un **superconjunto de Java 21** — y usar **Virtual Threads** (GA desde Java 21, JEP 444) donde el patrón sea bloqueante-por-petición y dominado por I/O (p. ej. endpoints REST, ciertos listeners), para alta concurrencia con código de estilo síncrono.
- Aprovechar capacidades finalizadas o maduradas desde 21 que encajan con este diseño: **Scoped Values** (propagación de contexto como `traceId`/`correlationId` a través de Virtual Threads, sin las fugas de `ThreadLocal`), **Structured Concurrency** (aún en preview; útil para el *fan-out* concurrente del orquestador si se necesita) y los GC generacionales de pausa baja (ZGC/Shenandoah).
- Fijar Java 25 en el build con **`maven-toolchains-plugin`**, desacoplando la versión de compilación del `JAVA_HOME`/`PATH` del entorno y garantizando reproducibilidad. Usar **Spring Boot 4.0.x** (Spring Framework 7, Spring Cloud 2025.1 Oakwood), la generación con soporte *first-class* de Java 25.

**Consecuencias**

*Positivas*
- Alta concurrencia en cargas de I/O sin pasar a programación reactiva: código síncrono legible que escala.
- Build reproducible y determinístico respecto a la versión de Java en cualquier máquina/CI.
- Acceso a mejoras del lenguaje (records, pattern matching, sealed, **Scoped Values**) que simplifican eventos, máquina de estados y propagación de contexto de tracing.
- **Horizonte de soporte LTS más largo** que Java 21, con coste de migración prácticamente nulo (25 es superconjunto de 21); además el JDK 25 ya está instalado en el entorno de desarrollo.

*Negativas*
- Virtual Threads no aceleran trabajo CPU-bound y tienen aristas conocidas: **pinning** por `synchronized` o llamadas JNI, y librerías/drivers no del todo preparados; requiere revisar el stack (driver JDBC, cliente Kafka) y preferir locks de `java.util.concurrent`.
- Java 25 + Spring Boot 4.0.x exige verificar que las dependencias fuera de BOM (Confluent/Avro, Resilience4j vía Spring Cloud, Spring Cloud Contract) certifiquen Boot 4 / JDK 25; ecosistema algo menos rodado (Jackson 3, Spring Framework 7), aunque soportado. Detalle de riesgos en [TECH-STACK.md](./TECH-STACK.md).
- `maven-toolchains-plugin` exige tener instalado el JDK 25 y un `toolchains.xml` configurado; un paso de setup adicional (documentado para "build en un comando").
- Virtual Threads no sustituyen control de concurrencia ni backpressure; pueden amplificar presión aguas abajo si no se limita (de ahí rate limiter en [ADR-05](#adr-05-dlq--retry-con-backoff-exponencial)).

**Alternativas consideradas**
- *Java 21 (LTS anterior):* la decisión de la v0.1.0; ecosistema muy rodado. Descartada a favor de 25 por mayor horizonte de soporte LTS y por las capacidades finalizadas (Scoped Values), siendo la migración trivial al ser superconjunto.
- *Pool de hilos de plataforma clásico:* sencillo y conocido, pero limita concurrencia de I/O y desperdicia memoria con muchas conexiones lentas.
- *Programación reactiva (WebFlux/Reactor):* gran escalabilidad de I/O, pero alta complejidad cognitiva, depuración difícil y peor encaje con JPA bloqueante. Virtual Threads dan gran parte del beneficio con un modelo mental síncrono. Descartado como enfoque general.
- *No usar toolchains (confiar en el JDK del PATH):* frágil y no reproducible entre entornos/CI. Descartado.

### ADR-09: Una base de datos por servicio (database-per-service)

**Estado:** Accepted

**Contexto.** OrderFlow se compone de bounded contexts independientes (order, payment, inventory, notification). Compartir un único esquema/BD entre servicios crea acoplamiento de datos, contención y rompe la propiedad de los datos por contexto, dificultando despliegues y evoluciones independientes.

**Decisión.** Cada servicio posee **su propia base de datos PostgreSQL** (con su tabla `outbox` y su tabla `processed_messages`). Ningún servicio accede directamente a las tablas de otro; la única integración entre servicios es vía **eventos/comandos en Kafka**. La consistencia entre servicios es **eventual**, gobernada por la saga ([ADR-01](#adr-01-saga-orquestada-vs-coreografiada)).

**Consecuencias**

*Positivas*
- Encapsulamiento real: cada contexto es dueño de su esquema y puede evolucionarlo sin coordinar migraciones globales.
- Aislamiento de fallos y de carga: la contención de una BD no afecta a las demás.
- Refuerza fronteras de dominio y despliegue independiente; encaja con outbox por servicio.

*Negativas*
- **No hay joins ni transacciones cross-service**: consultas que cruzan contextos exigen composición en API, réplicas de lectura o vistas materializadas a partir de eventos.
- Consistencia solo eventual: el sistema debe tolerar ventanas en las que los datos de distintos servicios no coinciden (la saga y la idempotencia lo gestionan).
- Más instancias/esquemas Postgres que operar (mitigado en local con un contenedor por BD o esquemas separados en Docker Compose).
- Duplicación controlada de algunos datos de referencia entre servicios.

**Alternativas consideradas**
- *Base de datos compartida (shared database):* simple para consultas y transacciones, pero acopla servicios por el esquema, genera contención y anula el despliegue/evolución independiente. Es un anti-patrón para microservicios; descartado.
- *Esquemas separados en una misma instancia Postgres:* compromiso pragmático (aislamiento lógico, una sola instancia) aceptable para el entorno local/laptop; se documenta como variante de despliegue, manteniendo la regla de "nadie toca el esquema de otro".
- *Un solo servicio monolítico con una BD:* simplifica todo pero contradice el objetivo del proyecto (sistema event-driven multi-servicio). Fuera de alcance.

---

## 7. Estrategia de pruebas y criterios de aceptación

Esta sección define cómo verificamos que OrderFlow cumple sus requisitos funcionales (RF) y no funcionales (RNF). La estrategia prioriza pruebas rápidas y deterministas en la base, y reserva las pruebas costosas (E2E, carga, caos) para validar invariantes de la saga, idempotencia y resiliencia que solo emergen con la infraestructura real.

### 7.1 Pirámide de pruebas

```
                    /\
                   /  \      E2E saga + Carga + Caos        (pocas, lentas, alto valor)
                  /----\     k6/Gatling, Awaitility, kill
                 /      \
                / Contr. \   Contract testing (SCC)          (medias)
               /----------\  producer/consumer pacts
              /            \
             / Integracion  \ Testcontainers (Kafka/PG/Redis) (decenas)
            /----------------\ slices: @DataJpaTest, Kafka IT
           /                  \
          /     Unitarias      \ Dominio + Saga Orchestrator   (cientos, ms)
         /----------------------\ JUnit5 + Mockito, sin I/O
```

| Nivel | Qué se prueba | Herramientas | Alcance / aislamiento | Objetivo de cobertura |
|---|---|---|---|---|
| Unitarias | Lógica de dominio pura (agregado Order, máquina de estados, reglas de stock, cálculo de montos) y decisiones del Saga Orchestrator | JUnit 5, Mockito, AssertJ | Sin red, BD ni Kafka. Colaboradores mockeados | >= 85% líneas en paquetes `domain` y `saga` |
| Integración | Mapeo JPA, outbox (escritura atómica + relay), serialización Avro/Schema Registry, dedup Redis, producción/consumo Kafka | Testcontainers (Kafka, Postgres, Redis, Schema Registry), Spring Boot Test slices, Awaitility | Contenedores reales, un servicio a la vez | Rutas críticas de persistencia y mensajería |
| Contract | Compatibilidad de mensajes entre productor y consumidor (commands/events) | Spring Cloud Contract | Pact por par de servicios | Todos los topics consumidos |
| E2E / Caos / Carga | Saga completa multi-servicio, compensaciones, idempotencia bajo fallo, DLQ, RNF de throughput/latencia | Docker Compose, Awaitility, Toxiproxy, k6/Gatling | Sistema completo levantado | Camino feliz + cada compensación + RNF |

### 7.2 Pruebas unitarias

Foco en las dos zonas de mayor densidad lógica, ejecutables en milisegundos y sin infraestructura.

**Dominio (agregado `Order`):**
- Transiciones de estado válidas e inválidas de la máquina `PENDING -> CONFIRMED | CANCELLED`. Toda transición ilegal (p.ej. `CONFIRMED -> PENDING`) debe lanzar `IllegalStateTransitionException`.
- Invariantes: una orden `CONFIRMED` no puede cancelarse; un `Order` no puede crearse con líneas vacías o cantidades <= 0; el total se recalcula de forma consistente.
- Reglas de `inventory-service`: reservar no permite stock negativo; liberar es seguro aunque la reserva ya no exista (operación tolerante).

**Saga Orchestrator (order-service):**
- Tabla de decisión pura: dado el estado actual de la saga (`sagaStep`, ver [§5.1.1](#511-agregado-order-order-service)) y el evento entrante, se emite el comando correcto.

| `sagaStep` (estado saga) | Evento entrante | Comando/acción esperada |
|---|---|---|
| `CREATED` | (arranque tras `OrderCreated`) | `ReserveInventory` |
| `INVENTORY_REQUESTED` | `InventoryReserved` | `ProcessPayment` |
| `INVENTORY_REQUESTED` | `InventoryReservationFailed` | `CancelOrder` -> `OrderCancelled` |
| `PAYMENT_REQUESTED` | `PaymentAuthorized` | `ConfirmOrder` -> `OrderConfirmed` |
| `PAYMENT_REQUESTED` | `PaymentDeclined` | `ReleaseInventory` (compensación) |
| `COMPENSATING` | `InventoryReleased` | `CancelOrder` -> `OrderCancelled` |

- Manejo de eventos fuera de orden o duplicados a nivel de orquestador (un `InventoryReserved` repetido no debe emitir dos `ProcessPayment`): se valida con el `processedMessages`/versión de saga mockeado.
- Eventos en estado terminal se ignoran (idempotencia de la máquina): un `PaymentAuthorized` que llega cuando la orden ya está `CANCELLED` no produce efectos.

### 7.3 Pruebas de integración con Testcontainers

Cada servicio levanta solo los contenedores que necesita. Se reutiliza el patrón de contenedor singleton estático para acortar el tiempo de suite. `Awaitility` se usa para esperar efectos asíncronos en lugar de `Thread.sleep`.

| ID | Escenario | Contenedores | Verificación |
|---|---|---|---|
| IT-01 | Outbox atómico | Postgres | `POST /orders` persiste `Order(PENDING)` y fila en `outbox` en la misma transacción; si la inserción del evento falla, hace rollback del estado |
| IT-02 | Relay publica y marca | Postgres, Kafka, Schema Registry | El poll-publisher lee `outbox`, publica en `order.events.v1`, marca la fila como enviada; reinicio del relay no republica filas ya marcadas (o las republica de forma idempotente) |
| IT-03 | Serialización Avro | Kafka, Schema Registry | Productor y consumidor intercambian `OrderCreated` Avro; evolución compatible de esquema (campo opcional añadido) no rompe consumidores |
| IT-04 | Dedup en consumidor | Kafka, Postgres, Redis | Reentrega del mismo `messageId` (mismo `consumerName`) no reprocesa: segundo intento no inserta en `processed_messages` ni dispara efecto de negocio |
| IT-05 | Partición por orderId | Kafka | Mensajes con misma `key=orderId` van a la misma partición y se consumen en orden; órdenes distintas se paralelizan |
| IT-06 | DLQ + retry | Kafka | Tras N reintentos con backoff, el mensaje envenenado aterriza en `<topic>.dlq` con headers de causa (`exception-fqcn`, `stacktrace`, `original-topic`) |
| IT-07 | Idempotencia de pago | Postgres, Redis | `payment-service` recibe dos `ProcessPayment` con misma clave de idempotencia y autoriza el cobro una sola vez |
| IT-08 | Lock distribuido | Redis | Reserva concurrente del mismo SKU bajo lock Redis no produce sobre-reserva |

### 7.4 Pruebas de la saga end-to-end con Awaitility

Se levanta el stack vía Docker Compose (o un `@SpringBootTest` multi-módulo con Testcontainers compartiendo red). Se publica `POST /orders` y se afirma el estado final consultando `GET /orders/{id}` y los eventos terminales, esperando con `Awaitility` (timeout 5s, poll 100ms).

```java
@Test
void happyPath_ordenSeConfirma() {
  var orderId = api.createOrder(validOrder());           // POST /orders
  await().atMost(5, SECONDS).untilAsserted(() ->
      assertThat(api.getOrder(orderId).status()).isEqualTo("CONFIRMED"));
  // y se observo NotificationSent para la orden
  assertThat(consumed("notification.events.v1", orderId)).contains("NotificationSent");
}
```

Escenarios obligatorios (cada uno afirma estado final + secuencia de eventos esperada):

| ID | Escenario | Estado final | Secuencia clave de eventos/comandos |
|---|---|---|---|
| E2E-01 | Camino feliz | `CONFIRMED` | OrderCreated -> ReserveInventory -> InventoryReserved -> ProcessPayment -> PaymentAuthorized -> ConfirmOrder -> OrderConfirmed -> NotificationSent |
| E2E-02 | Fallo de reserva | `CANCELLED` | OrderCreated -> ReserveInventory -> InventoryReservationFailed -> OrderCancelled -> NotificationSent |
| E2E-03 | Pago rechazado (compensación) | `CANCELLED` | ... InventoryReserved -> ProcessPayment -> PaymentDeclined -> ReleaseInventory -> InventoryReleased -> OrderCancelled -> NotificationSent |
| E2E-04 | Trazabilidad | `CONFIRMED` | Un único `traceId` atraviesa los 4 servicios (verificado en el backend de traces o en headers propagados) |

Para E2E-02 y E2E-03 se fuerza el comportamiento vía datos de prueba: SKU sin stock dispara `InventoryReservationFailed`; monto/tarjeta "mágica" dispara `PaymentDeclined` en el gateway simulado.

### 7.5 Contract testing entre servicios (Spring Cloud Contract)

Cada productor define contratos (DSL Groovy/YAML) que describen el shape de los mensajes que emite; SCC genera tests de verificación del lado productor y un stub/pact para el consumidor. Esto desacopla los servicios sin necesidad de un entorno integrado para detectar rupturas de contrato.

| Productor | Mensaje (topic) | Consumidor(es) que verifican el stub |
|---|---|---|
| order-service | OrderCreated, *Command (order/payment/inventory) | inventory-service, payment-service, notification-service |
| inventory-service | InventoryReserved, InventoryReleased, InventoryReservationFailed | order-service |
| payment-service | PaymentAuthorized, PaymentDeclined, PaymentRefunded | order-service |
| notification-service | NotificationSent | order-service (observabilidad) |

Reglas:
- El pipeline de CI falla si un cambio rompe un contrato consumido (campo requerido eliminado o tipo cambiado).
- Los contratos son la fuente de verdad de compatibilidad junto con el modo `BACKWARD` del Schema Registry; ambos deben coincidir.
- Mensajería (no HTTP): se usan los contratos de mensajería de SCC (`triggeredBy`/`outputMessage`).

### 7.6 Pruebas de resiliencia y caos

Validan las garantías que distinguen a OrderFlow: at-least-once + idempotencia, compensaciones y DLQ. Se ejecutan sobre el stack real con inyección de fallos (Toxiproxy para red, kill de contenedor para procesos).

| ID | Inyección de fallo | Resultado esperado |
|---|---|---|
| CH-01 | Matar `payment-service` (o `order-service`) a mitad de saga y reiniciarlo | La saga se reanuda desde el último punto durable (estado de saga + offsets de Kafka); el resultado final es idéntico al camino sin fallo y sin efectos duplicados (mismo único cobro, mismo único `OrderConfirmed`) |
| CH-02 | Entregar un evento duplicado (republicar manualmente el mismo `messageId`) | El consumidor lo descarta vía `processed_messages`/Redis; no hay segundo efecto de negocio (idempotencia) |
| CH-03 | Forzar `PaymentDeclined` | Se ejecuta la compensación completa (E2E-03): inventario liberado, orden `CANCELLED`; el stock vuelve a su valor original |
| CH-04 | Llenar la DLQ con mensajes envenenados (payload no deserializable) | Tras N reintentos con backoff exponencial, los mensajes van a `<topic>.dlq` con headers de causa; el consumidor principal no queda bloqueado y sigue procesando tráfico sano |
| CH-05 | Latencia/partición de red al Schema Registry o Kafka (Toxiproxy) | Resilience4j abre el circuito y aplica retry/backoff; al restaurar la red el sistema drena el backlog sin pérdida de eventos |
| CH-06 | Reinicio de Postgres durante el relay | El outbox garantiza que ningún evento confirmado se pierde; al volver, el relay publica lo pendiente exactamente como at-least-once |

Invariante transversal a verificar en todos: **cero pérdida de eventos** y **cero efectos secundarios duplicados** (un cobro por orden autorizada, una reserva neta por orden).

### 7.7 Pruebas de carga (k6 / Gatling)

Escenario base: ramp-up hasta sostener el target de throughput emitiendo `POST /orders`, midiendo la latencia extremo a extremo de la saga (de `OrderCreated` a estado terminal) mediante métricas Micrometer/Prometheus, excluyendo la latencia simulada del gateway de pago.

| Métrica | Target (RNF) | Cómo se mide |
|---|---|---|
| Throughput sostenido | >= 1.000 órdenes/min | Tasa de órdenes que alcanzan estado terminal durante 10 min |
| Latencia p99 de saga | < 2s (excl. gateway) | Histograma Micrometer de `saga.duration` |
| Tasa de error | < 0.1% bajo carga nominal | Respuestas 5xx + sagas atascadas |
| Pérdida de eventos | 0 | `ordenes creadas == ordenes terminales` al drenar |
| Lag de consumidor | Estable (no creciente) | `kafka.consumer.lag` en Prometheus |

Perfiles: (1) **carga nominal** sostenida 10 min; (2) **pico** 2x durante 1 min para observar elasticidad y recuperación de lag; (3) **soak** 1h para detectar fugas de memoria/conexiones. Se adjuntan los dashboards de Grafana como evidencia.

### 7.8 Definition of Done

Una unidad de trabajo (feature, servicio o historia) está "Done" cuando:

1. Código revisado y mergeado en `main` vía PR aprobado.
2. Pruebas unitarias y de integración nuevas/afectadas pasan; cobertura de dominio y saga >= 85%.
3. Contratos (SCC) actualizados y verdes; compatibilidad de esquema Avro `BACKWARD` confirmada.
4. Escenarios E2E afectados (camino feliz + compensaciones) pasan en CI.
5. Pruebas de caos relevantes (CH-01..CH-06) ejecutadas para cambios que tocan saga, outbox, idempotencia o DLQ.
6. Observabilidad presente: trazas extremo a extremo, métricas Micrometer y logs JSON con `traceId`/`correlationId`.
7. RNF de carga no regresan (throughput/latencia dentro de target en el escenario nominal).
8. Migraciones de BD versionadas (Flyway/Liquibase) y reproducibles.
9. `docker compose up` + un único comando de build levantan el sistema sin pasos manuales.
10. Documentación/README del módulo actualizada; pipeline de CI (build, test, escaneo, imagen) en verde.

### 7.9 Criterios de aceptación (numerados)

- **CA-01** Crear una orden válida (`POST /orders`) devuelve `201` con el `orderId` y deja la orden en `PENDING`, con el evento `OrderCreated` escrito en el outbox en la misma transacción.
- **CA-02** El camino feliz lleva la orden a `CONFIRMED` y emite `OrderConfirmed` y `NotificationSent` en < 2s p99 (excl. gateway).
- **CA-03** Ante stock insuficiente, la orden termina `CANCELLED` con `OrderCancelled` y sin cobro alguno.
- **CA-04** Ante `PaymentDeclined`, se ejecuta la compensación: `ReleaseInventory` -> `InventoryReleased`, la orden queda `CANCELLED` y el stock vuelve a su valor original.
- **CA-05** La reentrega de un evento con el mismo `messageId` no produce un segundo efecto de negocio (idempotencia verificada vía `processed_messages`/Redis).
- **CA-06** Matar y reiniciar un consumidor a mitad de saga reanuda el flujo hasta el mismo estado final, sin cobros ni reservas duplicados.
- **CA-07** Un mensaje que falla N reintentos aterriza en `<topic>.dlq` con headers de causa, sin bloquear el procesamiento del tráfico sano.
- **CA-08** Mensajes con `key=orderId` se procesan en orden por orden; órdenes distintas se procesan en paralelo.
- **CA-09** Un único `traceId` atraviesa los 4 servicios para una saga completa (100% de los pasos trazados).
- **CA-10** Bajo carga nominal el sistema sostiene >= 1.000 órdenes/min con tasa de error < 0.1% y lag de consumidor estable.
- **CA-11** Cero pérdida de eventos: tras drenar la carga, el número de órdenes creadas iguala al de órdenes en estado terminal.
- **CA-12** Un cambio que rompe un contrato consumido o la compatibilidad de esquema Avro hace fallar el pipeline de CI.
- **CA-13** El arranque local es reproducible: `docker compose up` + un comando de build, sin pasos manuales.

### 7.10 Matriz de trazabilidad (RF/RNF -> prueba -> criterio)

| Req | Descripción | Caso(s) de prueba | Criterio |
|---|---|---|---|
| RF-01, RF-02 | Crear orden + outbox atómico | IT-01, E2E-01 | CA-01 |
| RF-06..RF-09 | Saga hasta `CONFIRMED` | E2E-01, unit saga | CA-02 |
| RF-12 | Cancelación por fallo de inventario | E2E-02 | CA-03 |
| RF-13, RF-14 | Compensación por pago rechazado | E2E-03, CH-03 | CA-04 |
| RF-17, RF-18 | Consumidores idempotentes | IT-04, IT-07, CH-02 | CA-05 |
| RF-25 | Notificación en eventos terminales | E2E-01/02/03 | CA-02, CA-03 |
| RNF-12 | Reanudación sin duplicados | CH-01, CH-06 | CA-06 |
| RNF-18 | DLQ + retry con backoff | IT-06, CH-04 | CA-07 |
| RNF-06 | Orden por orden vía partición | IT-05 | CA-08 |
| RNF-22 | Trazabilidad extremo a extremo | E2E-04 | CA-09 |
| RNF-01 | >= 1.000 órdenes/min | Carga (perfil nominal) | CA-10 |
| RNF-02 | p99 saga < 2s | Carga (perfil nominal) | CA-02, CA-10 |
| RNF-10 | At-least-once + outbox (cero pérdida) | IT-02, CH-06, carga (drenaje) | CA-11 |
| RNF-31, RNF-35 | Compatibilidad inter-servicio | SCC ([§7.5](#75-contract-testing-entre-servicios-spring-cloud-contract)), IT-03 | CA-12 |
| RNF-38 | Build + compose reproducible | DoD #9, smoke CI | CA-13 |

---

## 8. Roadmap, riesgos y operabilidad

### 8.1 Roadmap por fases (vertical slices)

Cada fase entrega algo funcionando **end-to-end** y demostrable (un comando + una llamada REST), no capas horizontales a medias. El criterio de "hecho" de cada fase es: arranca con `docker compose up`, se prueba con un flujo real y queda cubierto por al menos un test de integración con Testcontainers.

| Fase | Objetivo | Entregables | Patrones que demuestra |
|------|----------|-------------|------------------------|
| **F0** | Scaffolding + infra reproducible | Monorepo Maven multi-módulo (`order/payment/inventory/notification-service`, `events-schema`, `common-lib`); `maven-toolchains-plugin` fijando Java 25; `docker-compose.yml` con Kafka (KRaft), Schema Registry, Postgres x4, Redis, AKHQ, Prometheus, Grafana; topics `.v1` (6 particiones) y `.dlq` creados al arranque; healthchecks de Compose | Build reproducible, infra como código, KRaft sin ZooKeeper |
| **F1** | Crear orden + outbox + evento publicado | `POST /orders` persiste `Order(PENDING)` + fila `OrderCreated` en tabla `outbox` **en la misma transacción**; relay poll-publisher publica a `order.events.v1`; Avro registrado en Schema Registry; `GET /orders/{id}` | **Transactional Outbox**, escritura atómica estado+evento, serialización Avro |
| **F2** | Saga feliz: inventario + pago | Orchestrator en order-service: `OrderCreated → ReserveInventory → InventoryReserved → ProcessPayment → PaymentAuthorized → ConfirmOrder → Order(CONFIRMED)`; inventory-service y payment-service (gateway simulado) funcionales; partición por `orderId` | **Saga orquestada** (camino feliz), comando/evento, orden por clave |
| **F3** | Compensaciones | Rama de fallo de inventario (`InventoryReservationFailed → OrderCancelled`); compensación de pago declinado (`PaymentDeclined → ReleaseInventory → InventoryReleased → OrderCancelled`); notification-service emite `NotificationSent` ante eventos terminales | **Transacciones compensatorias**, máquina de estados de la saga |
| **F4** | Idempotencia + DLQ + resiliencia | Dedup en `processed_messages` (clave `consumerName + messageId`) y/o Redis; `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` con backoff exponencial → `<topic>.dlq` con headers de causa; Resilience4j (circuit breaker en gateway de pago, retry, rate limiter) | **Consumidores idempotentes**, at-least-once seguro, **DLQ + retry**, circuit breaker |
| **F5** | Observabilidad extremo a extremo | OpenTelemetry con `traceId` propagado entre servicios (incluso a través de Kafka headers); Micrometer → Prometheus; dashboards Grafana (saga, lag, DLQ); logging JSON con `trace/correlationId`; traces en Tempo/Jaeger | **Tracing distribuido**, métricas RED, correlación de logs |
| **F6** | Carga + CI/CD | Script k6/Gatling validando >= 1.000 órdenes/min y p99 < 2s; GitHub Actions (build, test con Testcontainers, escaneo, build de imagen); Spring Cloud Contract entre servicios | Pruebas de carga, contract testing, pipeline automatizado |

```
F0 ──► F1 ──► F2 ──► F3 ──► F4 ──► F5 ──► F6
infra  outbox saga   compen idem/  observ carga/
       +event feliz  -sación DLQ    -abilid CI
                                            ad
```

Estimación realista para un único senior part-time: F0-F1 ~1 semana, F2-F3 ~2 semanas, F4 ~1-1.5 semanas, F5 ~1 semana, F6 ~1 semana. Total orientativo: **6-7 semanas**. Las fases F4 y F5 son las que aportan más señal "production-style"; no recortarlas.

### 8.2 Métricas de éxito del proyecto

Refinan los objetivos `OBJ-NN` de [§1.4](#14-objetivos-medibles-del-proyecto) y se verifican según los métodos de [§3.9](#39-métodos-de-verificación-referencia).

| Métrica | Objetivo | Cómo se mide |
|---------|----------|--------------|
| Throughput sostenido | >= 1.000 órdenes/min en laptop | k6/Gatling, ratio de `OrderConfirmed`/min |
| Latencia p99 de la saga | < 2s (excluyendo latencia simulada del gateway) | histograma Micrometer `saga.duration` (creación → estado terminal) |
| Pérdida de eventos | 0 (cero) | reconciliación: nº de órdenes vs. eventos terminales; outbox sin filas huérfanas |
| Recuperación tras caída | Saga se reanuda sin efectos duplicados | test de caos: matar consumidor a media saga y verificar estado final único |
| Trazabilidad | 100% de pasos con un mismo `traceId` | inspección en Tempo/Jaeger del span raíz al terminal |
| Arranque reproducible | `docker compose up` + 1 comando build, sin pasos manuales | CI ejecuta el flujo en limpio |
| Tasa de mensajes en DLQ | < 0,1% en operación normal | métrica de tamaño de `<topic>.dlq` |
| Cobertura de integración | Toda transición de saga cubierta | tests Testcontainers + Awaitility |

### 8.3 Registro de riesgos

| Riesgo | Impacto | Probabilidad | Mitigación |
|--------|---------|--------------|------------|
| Sobre-ingeniería para un solo dev (alcance infinito) | Alto | Alta | Vertical slices; cada fase entregable y demoable; congelar catálogo de eventos (este SDD) |
| Recursos de la laptop insuficientes (Kafka + 4 Postgres + Redis + observabilidad) | Alto | Media | Perfiles de Compose (`core` vs `full`); límites de memoria por contenedor; reducir particiones en local si hace falta |
| Duplicados por at-least-once mal manejados | Alto | Media | Idempotencia obligatoria (F4) antes de declarar la saga "completa"; tests con reentrega forzada |
| Saga huérfana / "atascada" (un evento nunca llega) | Alto | Media | Timeouts por paso + estado `STUCK`; job de barrido que compensa o alerta; DLQ con reintento |
| Evolución incompatible de esquemas Avro | Medio | Media | Schema Registry en modo `BACKWARD`; tests de compatibilidad en CI; sufijo `.v1` para versionado de topic |
| Acoplamiento por orquestación centralizada (order-service único) | Medio | Baja | Orquestador delgado (solo decide/emite comandos); lógica de dominio en cada servicio |
| Crecimiento incontrolado de la DLQ enmascarando un bug | Medio | Media | Alerta sobre tamaño de DLQ; runbook ([§8.5](#85-operabilidad)); causa raíz en headers del mensaje |
| Flakiness de tests con Testcontainers (arranques lentos) | Medio | Media | Reutilización de contenedores (`testcontainers.reuse.enable`); Awaitility en vez de `sleep`; timeouts generosos en CI |
| Pérdida de outbox por relay caído mucho tiempo | Medio | Baja | Relay idempotente y reanudable (marca `published_at`); alerta sobre antigüedad máxima de filas no publicadas |
| Falta de tiempo para F5/F6 | Medio | Alta | Priorizar F5 (observabilidad) sobre F6; F6 puede reducirse a un workflow CI mínimo |

### 8.4 Preguntas abiertas / decisiones pendientes

1. **Relay de outbox: poll-publisher vs. CDC (Debezium).** Por defecto poll-publisher (más simple, sin componente extra). Debezium queda documentado como alternativa de mayor throughput ([ADR-02](#adr-02-transactional-outbox-vs-publicación-directa)); decisión a revisar si F6 no alcanza el objetivo de 1.000 órdenes/min.
2. **Backend de traces: Tempo vs. Jaeger.** Inclinación a **Tempo** por integración nativa con Grafana (un solo UI). Pendiente confirmar consumo de recursos en laptop.
3. **Dedup: Postgres (`processed_messages`) vs. Redis vs. ambos.** Decisión actual ([ADR-04](#adr-04-estrategia-de-idempotenciadeduplicación)): Postgres como fuente de verdad transaccional; Redis para locks distribuidos y dedup "rápido". Falta decidir TTL de las claves en Redis.
4. **Granularidad de topics de comandos.** ¿`payment.commands.v1` único o por tipo de comando? Decisión actual: uno por servicio (menos topics, key=`orderId`).
5. **Reintentos de la saga vs. compensación inmediata.** ¿Cuántos reintentos antes de compensar un `PaymentDeclined` transitorio? Pendiente fijar política (p. ej. 3 reintentos con backoff y luego compensar).
6. **Esquema de versionado de eventos.** `.v1` en el topic cubre cambios incompatibles; los cambios menores aditivos se resuelven con `optional`/`default` en Avro sin nuevo topic ([§5.5.1](#551-compatibilidad-en-schema-registry)). Confirmar política `BACKWARD`.
7. **Manejo de Virtual Threads bajo carga con JPA.** Riesgo de "pinning" con conexiones bloqueantes; pendiente medir en F6 si conviene limitar el pool o usar threads de plataforma en los listeners de Kafka.

### 8.5 Operabilidad

#### Health / readiness probes
- Spring Boot Actuator: `/actuator/health/liveness` y `/actuator/health/readiness`.
- **Readiness** depende de: conectividad a Postgres, a Kafka (consumer asignado) y a Schema Registry. Mientras el relay/consumidores no están listos, el servicio reporta `OUT_OF_SERVICE` y no recibe tráfico.
- **Liveness** no debe depender de Kafka/Postgres (evita reinicios en cascada por una dependencia caída).
- Compose y, en su caso, el orquestador, usan estas probes para `depends_on` con `condition: service_healthy`.

#### Graceful shutdown
- `server.shutdown=graceful` + `spring.lifecycle.timeout-per-shutdown-phase=20s`.
- Los listeners de Kafka (`KafkaListenerEndpointRegistry`) se detienen primero: dejan de hacer `poll`, terminan el batch en curso y commitean offsets. Sin commits a medias.
- El relay de outbox termina su ciclo de publicación actual antes de cerrar.
- Al ser **at-least-once + idempotente**, un cierre brusco como mucho reentrega; nunca pierde ni duplica efectos.

#### Runbook básico

**Si crece la DLQ (`<topic>.dlq`):**
1. Inspeccionar headers del mensaje (`kafka_dlt-exception-fqcn`, `-message`, `-stacktrace`) en AKHQ.
2. Clasificar: ¿error transitorio (red, dependencia caída) o veneno (payload/esquema inválido)?
3. Transitorio → re-publicar al topic original (herramienta de replay) una vez restaurada la dependencia.
4. Veneno → corregir el productor/esquema; descartar o archivar el mensaje; **no** re-publicar sin fix.
5. Revisar si el pico coincide con un deploy (rollback si aplica).

**Si un servicio se cae:**
1. Verificar `liveness/readiness` y logs (filtrar por `traceId` del último flujo afectado).
2. Kafka rebalancea las particiones del consumer group automáticamente; las órdenes en vuelo continúan en otra instancia (si hay réplicas) o al reiniciar.
3. Al reiniciar, los consumidores idempotentes reprocesan desde el último offset commiteado sin duplicar efectos.
4. Buscar sagas en estado `STUCK` (job de barrido) y decidir reintento/compensación manual si el timeout expiró.

**Si el lag de consumo crece:**
1. Confirmar en Grafana `kafka_consumer_lag` por grupo/partición.
2. Escalar instancias (hasta nº de particiones = 6) o revisar cuello de botella (gateway de pago, DB).

#### Dashboards y alertas clave

| Dashboard | Paneles |
|-----------|---------|
| Saga overview | órdenes/min por estado, p50/p95/p99 de duración de saga, tasa de compensaciones |
| Kafka | consumer lag por grupo, throughput por topic, tamaño de cada `.dlq` |
| Outbox | filas pendientes, antigüedad máxima sin publicar, ritmo del relay |
| Recursos/RED | rate, errors, duration por servicio; CPU/mem por contenedor |

| Alerta | Condición |
|--------|-----------|
| DLQ creciente | tamaño de cualquier `.dlq` aumenta de forma sostenida (> umbral en 5 min) |
| Lag alto | `consumer_lag` > umbral durante > 5 min |
| Outbox atascado | fila no publicada con antigüedad > 60s |
| Saga atascada | nº de órdenes `PENDING`/`STUCK` con edad > timeout de saga |
| Error rate | tasa de 5xx o de excepciones en listeners > umbral |
| Circuit breaker abierto | estado `OPEN` en el breaker del gateway de pago |

### 8.6 Pipeline CI/CD (GitHub Actions)

```
┌──────────┐  ┌──────────┐  ┌──────────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐
│  build   │─►│   test   │─►│ contract +   │─►│  scan    │─►│  image   │─►│  publish     │
│ (Java 25 │  │ (unit +  │  │ schema-compat│  │(deps +   │  │ (build + │  │ (push a GHCR │
│  toolch.)│  │ TCs)     │  │              │  │ SAST)    │  │  Trivy)  │  │  en main)    │
└──────────┘  └──────────┘  └──────────────┘  └──────────┘  └──────────┘  └──────────────┘
```

| Etapa | Acciones |
|-------|----------|
| **build** | `actions/setup-java@v4` (Temurin 25) + `maven-toolchains`; `mvn -B verify -DskipTests`; cache de `~/.m2` |
| **test** | Tests unitarios + integración con **Testcontainers** (Kafka, Postgres, Redis); `testcontainers.reuse.enable=true`; reporte JUnit; cobertura JaCoCo |
| **contract + schema** | Verificación **Spring Cloud Contract** entre servicios; compatibilidad Avro `BACKWARD` contra el Schema Registry de referencia |
| **scan** | `mvn dependency-check` / OWASP, SAST (CodeQL), lint; falla el pipeline ante vulnerabilidades altas |
| **image** | Build de imágenes (Jib o `Dockerfile`) por servicio; escaneo de imagen con **Trivy** |
| **publish** | Solo en `main`: push de imágenes a GHCR etiquetadas por SHA y `semver`; firma opcional (cosign) |

Disparadores: `pull_request` ejecuta build → test → contract → scan (gate de merge). `push` a `main` ejecuta además image → publish. Estrategia de **matrix** por módulo para paralelizar build/test y mantener el feedback < 10 min.

---

## 9. Anexos / Referencias

### 9.1 Patrones arquitectónicos aplicados

| Patrón | Dónde se usa en OrderFlow | Referencia conceptual |
|---|---|---|
| **Saga (orquestada)** | Coordinación del ciclo de vida de la orden con compensaciones, en `order-service` ([§4.4](#44-saga-orquestada-comandos-eventos-y-compensaciones), [ADR-01](#adr-01-saga-orquestada-vs-coreografiada)) | Saga Pattern — Chris Richardson, *microservices.io/patterns/data/saga.html* |
| **Transactional Outbox** | Escritura atómica estado+evento + relay poll-publisher ([§5.2](#52-esquemas-de-soporte-outbox-y-processed_messages-ddl), [ADR-02](#adr-02-transactional-outbox-vs-publicación-directa)) | Transactional Outbox — *microservices.io/patterns/data/transactional-outbox.html* |
| **Idempotent Consumer** | Dedup `(consumerName, messageId)` con `processed_messages` + Redis ([§5.2](#52-esquemas-de-soporte-outbox-y-processed_messages-ddl), [ADR-04](#adr-04-estrategia-de-idempotenciadeduplicación)) | Idempotent Consumer — *microservices.io/patterns/communication-style/idempotent-consumer.html* |
| **Dead Letter Queue (DLQ)** | `<topic>.dlq` tras N reintentos con backoff exponencial ([ADR-05](#adr-05-dlq--retry-con-backoff-exponencial)) | Spring Kafka — `DefaultErrorHandler` / `DeadLetterPublishingRecoverer` |
| **Database per Service** | Una BD Postgres por bounded context ([ADR-09](#adr-09-una-base-de-datos-por-servicio-database-per-service)) | Database per Service — *microservices.io/patterns/data/database-per-service.html* |
| **Change Data Capture (alternativa)** | Evolución futura del relay vía Debezium/WAL ([ADR-02](#adr-02-transactional-outbox-vs-publicación-directa)) | Debezium — *debezium.io* |
| **Circuit Breaker / Retry / Rate Limiter** | Resilience4j en llamadas al gateway de pago ([§3.4](#34-disponibilidad-y-resiliencia), [ADR-05](#adr-05-dlq--retry-con-backoff-exponencial)) | Circuit Breaker — Martin Fowler; *resilience4j.readme.io* |
| **Distributed Tracing** | `traceId` propagado E2E vía OpenTelemetry / headers Kafka ([§4.5](#45-flujo-de-una-orden-de-extremo-a-extremo), [§3.5](#35-observabilidad)) | OpenTelemetry — *opentelemetry.io*; W3C Trace Context |
| **Schema Evolution (BACKWARD)** | Avro + Confluent Schema Registry, `TopicRecordNameStrategy` ([§5.5](#55-versionado-compatibilidad-y-headers-estándar), [ADR-07](#adr-07-avro--schema-registry-vs-json)) | Confluent Schema Registry — *docs.confluent.io* |

### 9.2 Referencias tecnológicas

- **Java 25 (LTS) / Virtual Threads** — Virtual Threads GA desde Java 21 (JEP 444); Scoped Values finalizados en Java 25; *openjdk.org*.
- **Spring Boot 4.0.x / Spring Cloud 2025.1 (Oakwood) / Spring Kafka 4.0.5** — *spring.io*.
- **Apache Kafka (KRaft)** — *kafka.apache.org*.
- **Testcontainers / Awaitility / Spring Cloud Contract** — testing de integración, asincronía y contratos.
- **Micrometer + Prometheus + Grafana + Tempo/Jaeger** — pila de observabilidad.
- **k6 / Gatling** — pruebas de carga.
- **RFC 7807** — Problem Details for HTTP APIs (formato de error de la API REST, [§5.6.4](#564-formato-de-error-rfc-7807)).
- **ISO-4217** — códigos de moneda (`currency`).

---

## 10. Control de cambios del documento

| Versión | Fecha | Autor | Cambios |
|---------|-------|-------|---------|
| 0.3.0 | 2026-06-14 | Yeferson Córdoba (@yefersoncm) | Fijada la línea **Spring Boot 4.0.x** (Spring Cloud 2025.1 Oakwood) como base sobre Java 25, sustituyendo el rango "3.5.x+ / 4.0.x". Añadido documento **[TECH-STACK.md](./TECH-STACK.md)** con versiones *pinned*, estrategia de BOMs y riesgos de compatibilidad. Actualizadas restricciones de stack, ADR-08 y referencias tecnológicas. |
| 0.2.0 | 2026-06-14 | Yeferson Córdoba (@yefersoncm) | Migración del *target* de plataforma **Java 21 → Java 25 (LTS)**: ADR-08 reescrita y marcada como *supersede*; línea base ajustada a **Spring Boot 3.5.x+ (o 4.0.x)** por compatibilidad. Actualizadas todas las referencias (stack/restricciones, RNF-33 build/toolchains, `toolchains.xml`, índice de ADRs, pipeline CI con `setup-java` Temurin 25 y anexos tecnológicos). **Sin cambios** en dominio, saga, eventos, topics ni contratos. |
| 0.1.0 | 2026-06-14 | Yeferson Córdoba (@yefersoncm) | Versión inicial (Draft). Integración de las 8 secciones del SDD: introducción y objetivos, requisitos funcionales, requisitos no funcionales, arquitectura, modelo de dominio y contratos, ADRs (01–09), estrategia de pruebas y criterios de aceptación, roadmap/riesgos/operabilidad. Unificación de nombres canónicos de servicios, topics, eventos y comandos; armonización de códigos HTTP (`201 Created` en creación de orden), de los estados de saga (`sagaStep`) y de la matriz de trazabilidad. Añadidos anexos de patrones y este control de cambios. |

> **Política de versionado del documento:** SemVer propio. Cambios aditivos no disruptivos → incremento *minor*; correcciones → *patch*; cambios estructurales o de decisiones canónicas (nuevas ADRs que superseden) → *major*. Toda modificación de diseño se refleja primero aquí (la divergencia código-spec se trata como defecto).

