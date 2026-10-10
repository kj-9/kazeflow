# `kazeflow instructions`

Print the short Markdown development guide bundled with the installed kazeflow
version. It is useful before writing a flow or asking a coding agent to do so.

```console
kazeflow instructions
```

The command needs no `ENTRY` and does not load your application, invoke a
factory, or execute assets. It works offline in a core-only installation.
Markdown is written to stdout; success exits `0`. An unsupported argument exits
`2` with a diagnostic on stderr. An unavailable bundled resource is an
infrastructure error (`4`). This command has no `--format json` variant.

The guide covers CLI-discoverable assets and flows, import-time effects,
partition selection, and inspection commands. These pages remain the detailed
reference: [CLI overview](../cli.md), [Partitions](../partitions.md), and
[Trust boundary](../concepts/trust-boundary.md).
