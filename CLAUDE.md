# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## Project-Specific Guidelines

### General

- Follow the code style and workflow requirements in this file.

### Comment Style

- Use a unified commenting style: // English 中文.
- Write comments for method names, field names, variables, constants, and each step of logic within methods.
- Keep comments concise and meaningful; explain only key intent and avoid empty or repetitive descriptions.

### Rust Coding Guidelines

- Prefer small, focused modules with clear public boundaries.
- Keep public APIs explicit and stable: export only types and functions intended for users of the crate.
- Use strong types for domain concepts instead of plain strings when the value has semantic meaning.
- Prefer `Result<T, RustWingError>` for recoverable errors; avoid `panic!` in library code.
- Use `thiserror` for error definitions and preserve useful context in error variants.
- Avoid `unwrap` and `expect` outside tests unless the invariant is obvious and local.
- Keep async boundaries explicit. Use `tokio` primitives for async coordination and avoid blocking work inside async paths.
- Prefer bounded channels for connection write queues and define backpressure behavior clearly.
- Do not introduce global mutable state unless there is a strong framework-level reason.
- Keep serialization types compatible with `serde` and avoid ad hoc JSON string manipulation.
- Run `cargo fmt` after Rust code changes.
- Run targeted tests for behavior changes, and add tests for connection policy, routing, protocol, and error behavior when relevant.

### Code Workflow

- When modifying or adding code, add short and clear comments for key logic.
- When modifying code, preserve existing comments as much as possible.
- If an existing comment style does not match the project convention, it may be adjusted to the unified style.
- Do not delete existing comments unless necessary.

### Verification

- After writing or modifying Rust code, run `cargo fmt`.
- When behavior changes, run `cargo test` or the smallest relevant test target.
- When only documentation changes, no build is required.
