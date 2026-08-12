## ADDED Requirements

### Requirement: Searchable hierarchical documentation
The hosted documentation SHALL provide site search, hierarchical global navigation,
page-level section navigation, stable heading links, code-copy affordances, and a
responsive mobile navigation model across tutorial, guide, concept, reference,
example, and release content.

#### Scenario: User looks up one command option
- **WHEN** a user searches for or navigates to a specific CLI command or option
- **THEN** they can reach a command-scoped reference page and its relevant section
  without scanning the entire CLI contract

#### Scenario: User reads on a narrow screen
- **WHEN** the documentation is viewed at a mobile viewport
- **THEN** global and local navigation remain operable without consuming the page
  width or clipping the document content

### Requirement: Separated documentation modes
The hosted documentation SHALL distinguish sequential getting-started material,
task-oriented guides, explanatory concepts, exact CLI and Python reference,
runnable examples, and release compatibility information in its navigation and page
purpose.

#### Scenario: User moves from tutorial to reference
- **WHEN** a user completes the first reviewed flow and needs exact API or CLI detail
- **THEN** the site provides a clearly labeled reference path without duplicating
  the tutorial as the reference

### Requirement: Curated generated Python API reference
The hosted documentation SHALL render signatures and source documentation for the
supported public Python API and explicit SQLite adapter while excluding private
execution implementation details.

#### Scenario: User checks a public signature
- **WHEN** a user opens the Python API reference for a documented public symbol
- **THEN** its installed-source signature and docstring are included in the built
  documentation alongside curated usage and boundary guidance

### Requirement: Strict framework documentation build
The repository SHALL build the documentation with its locked documentation-only
dependencies in strict mode before publishing the generated GitHub Pages artifact.
Documentation tooling MUST NOT become a required kazeflow runtime dependency.

#### Scenario: Documentation source contains an unresolved reference
- **WHEN** a pull request produces a strict documentation build warning or error
- **THEN** the Pages validation fails and no deployment job publishes that artifact

#### Scenario: Core package is installed without documentation tooling
- **WHEN** the kazeflow wheel is installed normally or without dependencies
- **THEN** planning, execution, and CLI behavior do not require MkDocs, Material, or
  mkdocstrings

### Requirement: Stable hosted entry URLs
The generated site SHALL retain the established public documentation root and the
existing top-level getting-started, CLI, partition, and result/history page URLs.

#### Scenario: Existing reader follows a documented link
- **WHEN** a reader follows a previously published top-level Pages URL
- **THEN** the generated documentation serves the corresponding current content
  without requiring a repository Markdown fallback
