---
hide:
  - toc
---

<div class="hero" markdown>

<span class="release-badge">Latest alpha · 0.1.0a4</span>

# Make a growing script understandable before you run it.

kazeflow turns ordinary Python functions into a small, inspectable flow. Review the
dependency graph, explicitly approve execution, and keep a useful result—without a
service, daemon, or required runtime dependency.

[Run your first flow](getting-started.md){ .md-button .md-button--primary }
[View on GitHub](https://github.com/kj-9/kazeflow){ .md-button }

</div>

<div class="grid cards" markdown>

-   **Start in one file**

    Install kazeflow, declare two assets, inspect the plan, and deliberately run it.

    [Getting started :octicons-arrow-right-24:](getting-started.md)

-   **Review the work first**

    Inspect targets and dependencies in text, JSON, Mermaid, or DOT.

    [CLI and graph output :octicons-arrow-right-24:](cli.md)

-   **Rerun one slice**

    Select a date, region, file, or another script-defined partition key.

    [Partition guide :octicons-arrow-right-24:](partitions.md)

-   **Look up an exact contract**

    Find command options, exit statuses, and public Python signatures.

    [Reference :octicons-arrow-right-24:](api/index.md)

</div>

## Where kazeflow fits

Start with a script. When it grows into several dependent steps, keep the functions
plain Python and add enough structure to see the work order, choose targets, and
inspect the outcome. kazeflow is intentionally not a scheduler, remote-worker
system, database-backed platform, or sandbox.

!!! warning "Trust the Python entry before loading it"

    Every CLI command imports the supplied Python entry. Import-time code and an
    explicitly selected factory can run before kazeflow discovers the flow.
    `plan` does not invoke decorated asset bodies after loading; it does not make an
    untrusted script safe. [Understand the trust boundary](concepts/trust-boundary.md).

## Choose how to read

| You want to… | Start here |
| --- | --- |
| Run kazeflow for the first time | [Getting started](getting-started.md) |
| Complete a specific task | [Guides](guides/index.md) |
| Understand the execution model | [Concepts](concepts/index.md) |
| Check an exact command or signature | [CLI reference](cli.md) or [Python API](api/index.md) |
| Adapt a complete pattern | [Examples](examples/index.md) |
