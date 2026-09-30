# 🏭 Moyu Dark Factory: Autonomous Zero-Sum Payment Engine

[![Track: Pocketful](https://img.shields.io/badge/Track-Pocketful-blue.svg)](https://lablab.ai/ai-hackathons/wearedevelopers-hackathon)
[![Hackathon: WeAreDevelopers x BAND](https://img.shields.io/badge/Hackathon-WeAreDevelopers%20x%20BAND-red.svg)](https://lablab.ai/ai-hackathons/wearedevelopers-hackathon)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An autonomous software factory operating in **BAND Desktop**, orchestrating a 3-agent autonomous swarm (`@architect`, `@coder`, `@auditor`) to design, implement, stress-test, and verify a zero-sum, high-concurrency payment and settlement engine.

---

## 🏛️ Factory Overview

- **Orchestration Surface**: BAND Desktop (formerly Jam)
- **Agent Seats**:
  - `@architect`: Contract formulation, state invariant definitions, verification matrix.
  - `@coder`: High-performance TypeScript implementation, deterministic ledger, container packaging.
  - `@auditor`: Autonomous gatekeeping, race condition simulation, fuzzing, and adversarial attacks.
- **Autonomous Protocol**: Human operator dispatches only the initial stage requirements. All planning, coding, static review, red-teaming, and defect rectification occur autonomously through peer-to-peer `@handle` messaging in the BAND room.

---

## 📁 Repository Structure

```
.
├── FACTORY.md            # Comprehensive factory whitepaper, cost metrics, and self-healing analysis
├── README.md             # Project documentation and stage walkthrough
├── mandates/             # Generic, domain-independent seat mandates
│   ├── architect.md
│   ├── coder.md
│   └── auditor.md
├── room.json             # Immutable export of the complete BAND Desktop room event log
├── stage-1/              # Stage 1: Core JSON API, zero-sum ledger, and idempotent settlements
└── stage-2/              # Stage 2: Web console, funds hold/capture, and offline state recovery
```

---

## 🚀 Stage Walkthrough

### Stage 1: Core API & Zero-Sum Accounting
- **Double-Entry Ledger**: Balances strictly conserve the seeded total under all operations.
- **Concurrency & Idempotency**: Atomic state transactions with strict `Idempotency-Key` deduplication.
- **Container Isolation**: 100% self-contained Docker image serving `/health` within 60s without outbound network access.
