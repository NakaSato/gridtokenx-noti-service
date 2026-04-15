---
name: rust-architecture
description: Guidance for structuring Rust projects at scale — workspace layout, crate splitting, module organization, monolith-vs-microservices decisions, and module/error/async patterns. Use this skill whenever the user asks about how to structure, organize, lay out, or refactor a Rust project, mentions Cargo workspaces, multi-crate projects, monorepos, or microservices in Rust, asks where to put a piece of code, or asks for examples of well-organized Rust codebases to learn from. Trigger this even when the user phrases it casually ("how should I organize this?", "where should this live?", "is my structure OK?") and even when they're asking about a specific subpart (just modules, just errors, just async strategy) — the decisions are interlinked and benefit from the broader frame.
---

# Rust Architecture

A skill for helping users structure Rust projects from a single binary up to large multi-crate workspaces and microservices. The core idea: structure decisions are trade-offs, not rules. Help the user reason about their specific situation rather than handing them a template.

## Core Stance: Frameworks Over Prescriptions

Most published Rust structure advice is too prescriptive — it gives you a layout without telling you why, then breaks down when your situation differs. Resist that. For every decision, surface the trade-off, ask what the user is optimizing for, and recommend based on their answer.

Three questions worth holding throughout any architecture conversation:
1. **What does this enable that I can't get otherwise?** If a structural choice doesn't unlock something concrete, skip it.
2. **What's the cost of getting it wrong later?** Cheap-to-reverse decisions deserve less analysis than expensive ones.
3. **Will the next person understand why?** Structure that needs three layers of explanation is usually wrong.

## The First Decision: How Big Is This, Really?

Before any layout discussion, calibrate the scale. The right structure for 2,000 lines is not the right structure for 200,000.

| Situation | Recommended starting point |
|-----------|---------------------------|
| Single binary, <10k lines, solo dev | Single crate, well-organized modules. Don't split prematurely. |
| Library + CLI sharing code | Lib + bin in one package, or a 2-crate workspace |
| Multiple binaries sharing logic | Workspace with `bin/` and `crates/` |
| Long-lived service or product, multi-person | Layered workspace, 5–8 crates |
| Different runtime targets (e.g. on-chain `no_std` + off-chain tokio) | Workspace with primitives crate gated by features |
| Independent teams shipping independently | Microservices (only when this is genuinely true) |

Push back on premature splitting. Splitting is mechanical with `cargo`; reversing a sprawling micro-crate jungle is not.

## Workspace Fundamentals

When a workspace is justified, these fundamentals apply across every project:

**Virtual manifest at the root.** No code in the root `Cargo.toml`. The root has only `[workspace]` plus shared metadata. Putting a crate at the root pollutes commands and creates an awkward exception.

**Directory name == crate name.** `crates/storage/db/` houses the `<project>-storage-db` crate. Makes navigation, renames, and reverse-dependency search trivially greppable.

**One level of grouping under `crates/`.** Flat works up to ~20 crates; deep hierarchy is hard to discover. The middle ground — domain folders one level deep, each containing 1–5 closely related crates — scales to hundreds. This is the pattern reth uses with 180+ crates.

**Binaries separate from libraries.** Use `bin/` for executables, `crates/` for libraries. Anything that compiles to an executable goes in `bin/`. Anything else is a library.

**Shared metadata via `[workspace.package]` and `[workspace.dependencies]`.** Single source of truth for versions and edition. Each member writes `tokio.workspace = true` instead of pinning a version locally.

**Workspace-wide lints.** Rust 1.74+ supports `[workspace.lints]`. Use it. Each crate adds `[lints] workspace = true` and inherits.

## When to Split a Crate

Real reasons to extract a crate:
- **Compile time pain** on incremental rebuilds (>30s sustained)
- **Two binaries need to share code** (now you need at least lib + bin)
- **A heavy or platform-specific dependency** should stay isolated
- **Different stability guarantees** (public SDK vs internal services)
- **Different runtime targets** (on-chain `no_std` vs off-chain tokio, WASM vs native)
- **Plugin boundary** — third parties implement a trait you define

Bad reasons:
- "It feels cleaner"
- "I might need it later"
- "Hexagonal architecture says so"
- "Each domain should be its own crate"

Splitting too aggressively is the more common failure mode. A 30-crate workspace where every change requires touching 5 `Cargo.toml`s is a real cost.

## Layering: The Dependency Direction Rule

In any multi-crate project, enforce strict acyclic layering. Lower layers must never depend on higher ones:

```
   bin / api / worker        ← adapters, top of stack
        │
      services               ← orchestration, async, I/O
        │
   domain / protocol         ← pure logic, no I/O
        │
       primitives            ← types, errors, units (bottom)
```

The bottom layer should have minimal dependencies — ideally `no_std`-compatible — so it's reusable across runtimes (on-chain programs, WASM clients, tests). This is the single highest-leverage decision in many codebases.

## Module Layout Inside a Crate

Several styles work; consistency matters more than choice. Pick one per crate and stick with it.

**Flat, file-per-concept** — readable, easy to grep, good up to maybe 15 files:
```
src/
├── lib.rs
├── matcher.rs
├── settlement.rs
└── oracle.rs
```

**Hierarchical by feature** — scales further:
```
src/
├── lib.rs
├── order_book/
│   ├── matcher.rs
│   └── book.rs
└── settlement/
```

**Hexagonal (ports/adapters)** — heavier ceremony, pays off only when you have multiple adapters hitting the same logic:
```
src/
├── domain/
├── application/
├── ports/
└── adapters/
```

Use the modern `mod.rs`-free style: `order_book.rs` + `order_book/` directory rather than `order_book/mod.rs`. Less ambiguity in editors and grep.

`lib.rs` should be thin — declare modules, re-export the public API, define nothing substantive.

## Errors: Three Valid Approaches

There is no single right answer. Recommend based on what the user's callers actually need to do with errors.

- **`thiserror` everywhere** — typed errors callers can match on. Best for libraries and any code where caller logic depends on error variants. Cost: more boilerplate.
- **`anyhow` everywhere** — opaque errors with context. Fast to write, fine for binaries and prototypes. Cost: callers can't distinguish errors programmatically.
- **Hybrid** (typed inside, anyhow at edges) — `thiserror` in domain crates, `anyhow` in handlers and binaries. The most common mature choice.

Never `pub use anyhow::Error` from a library — it erases type information for consumers.

## Async Strategy

Three defensible positions:

- **Async at the edges only** — pure sync core, async I/O wrappers. Easiest to test (no runtime needed), best for CPU-bound logic like matching engines, parsers, computation.
- **Async throughout** — every potentially-I/O function is async. Common in services-heavy codebases. Watch for `async` infecting code that doesn't need it.
- **Runtime-agnostic** — define your own async traits. Right for libraries; overkill for applications.

Default recommendation for most projects: **sync core, async edges**. Pure logic shouldn't be `async`; only I/O wrappers should be.

## Visibility

Apply discipline proportional to the stability commitment:
- **Internal crate?** Default to `pub` for ergonomics, don't worry about it.
- **Crate consumed by other teams?** `pub(crate)` by default; `pub` is a contract.
- **Published library?** Every `pub` is a SemVer commitment. Use `#[doc(hidden)]` and sealed traits as escape hatches.

The mistake is applying library-grade discipline to internal code (slows you down) or internal-grade laxness to library code (breaks consumers).

## Feature Flags

Powerful and dangerous. Right for:
- Optional integrations (`postgres`, `redis`)
- Platform variants (`std` vs `no_std`)
- Heavy dependencies users can opt out of

Wrong for:
- Configuration that should be runtime
- A/B-style behavior switching
- "Modes" of operation

Rules to enforce: features must be **additive** (enabling a feature never removes anything), no `default` features in workspace internals (let consumers opt in), test combinations with `cargo hack`. If a crate has more than 4–5 features, ask whether some should be separate crates.

## Microservices: Decision Before Design

Microservices in Rust are a different problem from a monolithic workspace. They solve organizational and operational problems, not technical ones. Push back hard on premature adoption.

**Real reasons to use microservices:**
- Multiple teams need to ship independently
- Different scaling profiles (16-core matcher vs 0.5-core log writer)
- Different reliability requirements (settlement must never go down; reporting can)
- Different tech stacks needed
- Regulatory isolation (PII or sensitive operations need stricter access controls)

**Wrong reasons:**
- Team of 1–5 people (operational tax dwarfs benefits)
- Future-proofing without concrete near-term need
- Demo or hackathon timeline
- "Cleaner architecture"

**Default recommendation: build a modular monolith first.** The workspace structure with clean layering gives you internal boundaries that *can* become service boundaries later. Migration is mechanical when the pain is concrete; premature splitting is expensive to reverse.

## Microservice Structure (When Justified)

If splitting is justified, each service is its own workspace following the layered pattern:

```
matching-service/
├── Cargo.toml                # workspace root
├── bin/matching-service/     # the binary
├── crates/
│   ├── domain/               # pure logic
│   ├── application/          # use cases
│   ├── adapters/             # HTTP/gRPC, DB, message bus
│   └── contracts/            # this service's published API types
├── migrations/
├── deploy/
└── tests/
    ├── integration/
    └── contract/
```

**Cross-service code sharing — the hardest question.** Two failure modes:
- **Too much sharing**: a giant `shared` crate every service depends on. Now every service redeploys when it bumps. You've recreated the monolith.
- **Too little**: every service redefines types and retry logic. Drift, bugs, wasted effort.

The middle path:
- **Shared protocol crates** for wire formats (gRPC `.proto`, OpenAPI specs, event schemas). Versioned independently.
- **Shared `platform` crate** for cross-cutting concerns (tracing setup, config, health endpoints, metrics, auth). Stable, rarely changes.
- **No shared domain types.** If two services need an `Order`, each defines its own. The wire format is the contract; internal types are private.

**Communication patterns:**
- Synchronous (gRPC with `tonic`) for queries and operations where the caller needs the result
- Asynchronous (events via NATS/Kafka) for state-change notifications and fan-out
- The workhorse pattern: synchronous command, asynchronous events
- Avoid: chained synchronous calls more than 2 deep — that's a distributed monolith with terrible failure characteristics

**Resilience non-negotiables:** timeouts on every network call (use `tower::timeout`), retries with backoff for idempotent ops only, circuit breakers, idempotency keys on every state-changing operation, graceful shutdown handling SIGTERM.

**Observability is the substrate, not an add-on:** distributed tracing (OpenTelemetry + `tracing-opentelemetry`), structured JSON logging with consistent fields, metrics following RED method. Build this into the platform crate from day one.

**One database per service.** No shared tables, no cross-service joins. Cross-service queries via API composition, CQRS read models, or events-fed materialized views.

## Exemplary Projects to Study

Stars don't equal good code. These are the projects worth reading for architecture lessons:

- **`ripgrep`** — Burntsushi's projects are famously clean. Small enough to read end-to-end. Workspace with clear separation of concerns.
- **`reth`** — Modern Ethereum execution client, ~180 crates organized by domain under `crates/`. The reference for large-scale Rust workspaces. Read `docs/repo/layout.md`.
- **`foundry`** — Another excellent blockchain workspace; cleaner than most for studying CLI + libraries + shared types splits.
- **`uv` and `ruff`** (Astral) — widely cited as exemplars of modern workspace design. Heavy use of feature gating, careful crate boundaries, fast CI.
- **`tokio`** — study how a foundational library handles `no_std`, feature flags, and stable public APIs across many sub-crates.
- **`helix`** — mid-sized editor, good example of separating pure core (`helix-core`) from runtime (`helix-term`, `helix-view`, `helix-lsp`).
- **`rust-analyzer`** — one of the most architecturally interesting Rust codebases. Read its `ARCHITECTURE.md` — many projects copy the format because it's that good. Deliberately avoids "default Rust" anti-patterns (pervasive `Arc<Mutex<_>>`, `RefCell`).

When recommending which to study, match the user's context. Building a service? Read reth. Building a library? Read tokio. Building a CLI tool? Read ripgrep or foundry. Building anything large? Read rust-analyzer's `ARCHITECTURE.md`.

## Starter Templates

### Workspace `Cargo.toml`

```toml
[workspace]
resolver = "2"
members = ["bin/*", "crates/*", "xtask"]

[workspace.package]
version = "0.1.0"
edition = "2021"
rust-version = "1.80"
license = "Apache-2.0"

[workspace.dependencies]
# async / runtime
tokio = { version = "1.40", features = ["full"] }
# serialization
serde = { version = "1", features = ["derive"] }
# errors / logging
thiserror = "1"
anyhow = "1"
tracing = "0.1"

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
pedantic = { level = "warn", priority = -1 }
unwrap_used = "deny"
```

### Member crate `Cargo.toml`

```toml
[package]
name = "myproject-matching-core"
version.workspace = true
edition.workspace = true
license.workspace = true

[lints]
workspace = true

[dependencies]
serde.workspace = true
thiserror.workspace = true
```

### Recommended supporting files
- `rust-toolchain.toml` — pin compiler version
- `.cargo/config.toml` — aliases, lints, target settings
- `clippy.toml`, `rustfmt.toml`
- `deny.toml` (cargo-deny) when supply chain matters
- `ARCHITECTURE.md` — one page, layer diagram, crate inventory with one-sentence purposes

## Tooling: Pick Your Floor

Minimum viable: `rustfmt` + `clippy` in CI. Add others as needs arise — don't adopt the full suite on day one. Each tool is an ongoing tax.

- `cargo-deny` — supply chain hygiene
- `cargo-hack` — feature combination testing
- `cargo-nextest` — faster test runner
- `cargo-machete` — find unused dependencies
- `cargo-chef` — Docker layer caching for service builds
- `xtask` pattern — repo automation in Rust instead of shell scripts

## Common Pitfalls to Watch For

When reviewing a user's structure, check for these failure modes:

- **Cyclic crate dependencies** — won't compile but the user may try anyway
- **A `utils` or `common` crate that everything depends on** — usually means the layering is wrong
- **Domain types in a `shared` crate across services** — recreates the monolith
- **`pub` everywhere by default** — locks in API contracts that will hurt later
- **Async in pure logic crates** — makes testing harder for no benefit
- **Default features that aren't truly default** — surprises consumers
- **No `ARCHITECTURE.md`** — large codebases without one become navigable only to the original authors
- **`unwrap()` and `expect()` in library code** — should be a clippy deny

## How to Run a Conversation

When a user asks for structural advice:

1. **Calibrate scale first.** Ask roughly how big the codebase is or will be, how many people, what's already built. Avoid prescribing without this.

2. **Identify what they're optimizing for.** Compile time? Reusability? Independent deployment? Onboarding new contributors? The answer changes the recommendation.

3. **Recommend the smallest structure that fits.** Easier to grow into a structure than to dismantle one. If unsure between two options, recommend the simpler one and note when to revisit.

4. **Show the trade-offs.** Don't hand a layout without explaining why this layout, what it costs, and what would change the answer.

5. **Be concrete.** Show actual `Cargo.toml` snippets and tree layouts when discussing structure. Reference specific exemplary projects to read rather than abstract principles.

6. **Suggest `ARCHITECTURE.md` as the first artifact.** Even a one-page version pays off immediately and forces the user to articulate the layering before code locks it in.