# LedgerFlow

> A production-oriented, event-sourced financial ledger engineered for correctness under concurrency, fault tolerance, and distributed-system scale.

LedgerFlow is a backend system that models financial transactions using **immutable double-entry ledger events** rather than mutable account balances.

The system is designed to explore how real financial infrastructure handles:

- Financial correctness
- ACID transactions
- Double-entry bookkeeping
- Concurrency
- Idempotency
- Event sourcing
- Auditability
- Point-in-time state reconstruction
- Caching
- Event-driven architecture
- Reliable event publishing
- Observability
- Load testing
- Failure recovery
- Distributed systems

The goal is not to build a simple banking CRUD API, but to engineer a system where **money cannot be accidentally created, duplicated, or lost even when requests arrive concurrently or infrastructure components fail.**

---

## Table of Contents

- [Architecture](#architecture)
- [Core Design Principles](#core-design-principles)
- [Technology Stack](#technology-stack)
- [System Components](#system-components)
- [Data Model](#data-model)
- [Event-Sourced Ledger](#event-sourced-ledger)
- [Double-Entry Bookkeeping](#double-entry-bookkeeping)
- [Account Lifecycle](#account-lifecycle)
- [Financial Operations](#financial-operations)
- [Balance Reconstruction](#balance-reconstruction)
- [Concurrency Control](#concurrency-control)
- [Idempotency](#idempotency)
- [Auditability](#auditability)
- [Point-in-Time Balance](#point-in-time-balance)
- [Snapshots](#snapshots)
- [Redis Caching](#redis-caching)
- [Kafka Event Streaming](#kafka-event-streaming)
- [Transactional Outbox](#transactional-outbox)
- [Failure Handling](#failure-handling)
- [Observability](#observability)
- [Testing Strategy](#testing-strategy)
- [Load Testing](#load-testing)
- [Performance](#performance)
- [Project Structure](#project-structure)
- [Local Development](#local-development)
- [API](#api)
- [Database Setup](#database-setup)
- [Running the Application](#running-the-application)
- [Example Flow](#example-flow)
- [Financial Invariants](#financial-invariants)
- [Failure Scenarios](#failure-scenarios)
- [Engineering Trade-offs](#engineering-trade-offs)
- [Roadmap](#roadmap)
- [Future Improvements](#future-improvements)

---

# Architecture

LedgerFlow follows an event-sourced, transactionally consistent architecture.

```text
                         ┌──────────────────────┐
                         │       Client         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │      FastAPI         │
                         │      REST API        │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │   Ledger Service     │
                         │                      │
                         │ Validation           │
                         │ Business Rules       │
                         │ Idempotency          │
                         │ Concurrency Control  │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │     PostgreSQL       │
                         │                      │
                         │ Accounts             │
                         │ Transactions         │
                         │ Ledger Entries       │
                         │ Outbox Events        │
                         └──────────┬───────────┘
                                    │
                         ┌──────────┴───────────┐
                         │                      │
                         ▼                      ▼
                  ┌──────────────┐       ┌──────────────┐
                  │    Redis     │       │    Outbox    │
                  │    Cache     │       │   Publisher  │
                  └──────────────┘       └──────┬───────┘
                                                │
                                                ▼
                                         ┌──────────────┐
                                         │    Kafka     │
                                         │ Event Stream │
                                         └──────┬───────┘
                                                │
                              ┌─────────────────┼─────────────────┐
                              │                 │                 │
                              ▼                 ▼                 ▼
                        Notifications     Analytics        Audit/Consumers


                 ┌──────────────────────────────────────────┐
                 │          Observability Layer              │
                 │                                          │
                 │ Prometheus → Metrics                     │
                 │ Grafana → Dashboards                    │
                 │ Logs → Application/Infrastructure        │
                 └──────────────────────────────────────────┘