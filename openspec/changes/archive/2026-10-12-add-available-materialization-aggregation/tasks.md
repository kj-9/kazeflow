## 1. Optional collection contract (root)

- [x] 1.1 Add optional protocol/export and independent collection evidence; verify existing adapters, explicit scope/order/falsey keys, partial failure and load-time revocation.

## 2. File collection (adapter owner)

- [x] 2.1 Implement bounded available(contexts) using existing resolve semantics; verify dedicated validation and file adapter regressions without format/version changes.

## 3. Application publication and guidance (Docs owner + independent test owner)

- [x] 3.1 Add partial aggregation/atomic receipt example and documentation; verify example repeated with existing root and strict Docs.
- [x] 3.2 Verify atomic publication failure/cleanup/idempotent receipt and no producer-status rewrite using dedicated publication tests.

## 4. Final integration (root serial)

- [x] 4.1 Review scenarios and full tests/lint/types/Docs; verify integration and supported CI matrix.
- [x] 4.2 Build wheel, metadata and isolated core install; verify packaged modules match source and installed collection smoke.
- [x] 4.3 Doctor/strict verification, sync/archive/roadmap; verify living specs and scoped commit preparation.
