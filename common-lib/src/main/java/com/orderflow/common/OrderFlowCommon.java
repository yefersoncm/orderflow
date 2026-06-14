package com.orderflow.common;

/**
 * Marcador del modulo de utilidades compartidas de OrderFlow.
 *
 * <p>En fases posteriores aqui viviran: relay de la tabla outbox, soporte de
 * idempotencia/deduplicacion, propagacion de contexto de tracing y plumbing de DLQ.
 */
public final class OrderFlowCommon {

    private OrderFlowCommon() {
        // Clase de utilidad: no instanciable.
    }
}
