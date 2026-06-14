-- Inicializacion de PostgreSQL para OrderFlow (entorno local).
-- Database-per-service (ADR-09): una BASE DE DATOS logica por servicio,
-- compartiendo una sola instancia Postgres para simplicidad en desarrollo.
CREATE DATABASE orderdb;
CREATE DATABASE paymentdb;
CREATE DATABASE inventorydb;
CREATE DATABASE notificationdb;

-- El usuario 'orderflow' (POSTGRES_USER) es owner por defecto de todas.
