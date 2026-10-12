"""Explicit local file storage for reusable materialized asset outputs.

The adapter stores outputs only when its ``commit`` method is called. A codec
must be selected explicitly so applications decide which values are portable
and how their bytes are produced.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
import re
import tempfile
import uuid
from datetime import date
from pathlib import Path
from collections.abc import Sequence
from typing import Any, Protocol

from kazeflow.materialization import MaterializationContext, MaterializationRecord


class FileMaterializationError(Exception):
    """A stored reference or generation is malformed, incompatible, or damaged."""

    def __init__(self, *args: object, reason: str = "corrupt") -> None:
        super().__init__(*args)
        self.reason = reason


class MaterializationCodec(Protocol):
    """Explicit byte encoding used by :class:`FileMaterialization`."""

    codec_id: str

    def encode(self, output: Any) -> bytes:
        """Encode an output into bytes."""
        ...

    def decode(self, payload: bytes) -> Any:
        """Decode previously encoded bytes."""
        ...


class JsonCodec:
    """Strict JSON codec that accepts only losslessly representable builtins."""

    codec_id = "kazeflow.json.v1"

    def encode(self, output: Any) -> bytes:
        _validate_json_value(output, set())
        try:
            text = json.dumps(
                output,
                ensure_ascii=True,
                allow_nan=False,
                separators=(",", ":"),
            )
            return text.encode("utf-8")
        except (TypeError, ValueError, UnicodeError) as exc:
            raise ValueError(f"output cannot be encoded as strict JSON: {exc}") from exc

    def decode(self, payload: bytes) -> Any:
        if type(payload) is not bytes:
            raise TypeError("JSON payload must be bytes")
        try:
            text = payload.decode("utf-8")
            value = json.loads(
                text,
                parse_constant=_reject_json_constant,
                object_pairs_hook=_unique_json_object,
            )
            _validate_json_value(value, set())
            return value
        except (
            UnicodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
            RecursionError,
        ) as exc:
            raise ValueError(f"stored payload is not strict JSON: {exc}") from exc


def _reject_json_constant(value: str) -> Any:
    raise ValueError(f"non-finite JSON number {value} is not allowed")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _validate_json_value(value: Any, active: set[int]) -> None:
    value_type = type(value)
    if value is None or value_type in (str, bool, int):
        return
    if value_type is float:
        if not math.isfinite(value):
            raise ValueError("JSON values cannot contain non-finite floats")
        return
    if value_type is list:
        identity = id(value)
        if identity in active:
            raise ValueError("JSON values cannot contain cycles")
        active.add(identity)
        try:
            for item in value:
                _validate_json_value(item, active)
        finally:
            active.remove(identity)
        return
    if value_type is dict:
        identity = id(value)
        if identity in active:
            raise ValueError("JSON values cannot contain cycles")
        active.add(identity)
        try:
            for key, item in value.items():
                if type(key) is not str:
                    raise TypeError("JSON object keys must be strings")
                _validate_json_value(item, active)
        finally:
            active.remove(identity)
        return
    raise TypeError(
        "strict JSON supports only None, exact str/bool/int/finite float, "
        "list, and dict values; tuples and custom types are unsupported"
    )


_HEX_ID = re.compile(r"^[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_REF_FIELDS = {"schema_version", "materialization_id"}
_REF_FIELDS_V2 = _REF_FIELDS | {"epoch"}
_MANIFEST_FIELDS = {
    "schema_version",
    "materialization_id",
    "identity",
    "codec_id",
    "payload_size",
    "payload_sha256",
}
_MANIFEST_FIELDS_V2 = _MANIFEST_FIELDS | {"epoch"}
_LEDGER_FIELDS = {"schema_version", "identity", "epoch", "reason"}


@dataclass(frozen=True, slots=True)
class FileMaterializationInspection:
    """Read-only diagnosis of one exact file materialization identity."""

    status: str
    record: MaterializationRecord | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {
            "available",
            "missing",
            "invalidated",
            "incompatible",
            "corrupt",
        }:
            raise ValueError("inspection status is invalid")
        if self.status == "available":
            if (
                not isinstance(self.record, MaterializationRecord)
                or self.record.committed is not True
            ):
                raise ValueError("available inspection requires a committed record")
            if self.reason is not None:
                raise ValueError("available inspection cannot have a reason")
        else:
            if self.record is not None:
                raise ValueError("non-available inspection cannot have a record")
            if self.status == "missing":
                if self.reason is not None:
                    raise ValueError("missing inspection cannot have a reason")
            elif type(self.reason) is not str or not self.reason:
                raise ValueError("inspection diagnosis requires a reason")


class FileMaterialization:
    """Store immutable generations under a local root with atomic references.

    ``root`` is trusted local application storage. A missing root is treated as
    a cache miss by :meth:`resolve`; :meth:`commit` and :meth:`invalidate` may
    create directories when explicitly called.
    """

    def __init__(self, root: str | Path, *, codec: MaterializationCodec) -> None:
        if not isinstance(root, (str, Path)):
            raise TypeError("root must be a path string or pathlib.Path")
        if not callable(getattr(codec, "encode", None)) or not callable(
            getattr(codec, "decode", None)
        ):
            raise TypeError("codec must provide callable encode and decode methods")
        codec_id = getattr(codec, "codec_id", None)
        if type(codec_id) is not str or not codec_id.strip():
            raise ValueError("codec.codec_id must be a non-empty string")
        self.root = Path(root)
        self.codec = codec
        self._codec_id = codec_id

    def resolve(self, context: MaterializationContext) -> MaterializationRecord | None:
        """Resolve and validate the currently published generation, if any."""
        identity = _identity(context)
        inspection = self._inspect(context, identity)
        if inspection.status in ("missing", "invalidated"):
            return None
        if inspection.status in ("corrupt", "incompatible"):
            raise FileMaterializationError(
                inspection.reason or "materialization is unavailable",
                reason=inspection.status,
            )
        return inspection.record

    def available(
        self, contexts: Sequence[MaterializationContext]
    ) -> tuple[MaterializationRecord, ...]:
        """Return committed records for an explicit, ordered identity selection.

        The selection is snapshotted and every identity is validated before any
        storage lookup. Missing or invalidated identities are omitted; storage
        corruption and incompatibility errors from :meth:`resolve` propagate.
        """
        if isinstance(contexts, (str, bytes, bytearray)) or not isinstance(
            contexts, Sequence
        ):
            raise TypeError("contexts must be a non-text sequence of contexts")

        selected = tuple(contexts)
        identities: set[bytes] = set()
        for context in selected:
            identity_bytes = _canonical_json(_identity(context))
            if identity_bytes in identities:
                raise ValueError("contexts contain a duplicate exact identity")
            identities.add(identity_bytes)

        records: list[MaterializationRecord] = []
        for context in selected:
            record = self.resolve(context)
            if record is not None:
                records.append(record)
        return tuple(records)

    def inspect(self, context: MaterializationContext) -> FileMaterializationInspection:
        """Inspect one identity without writing or decoding its output."""
        identity = _identity(context)
        return self._inspect(context, identity)

    def _inspect(
        self, context: MaterializationContext, identity: dict[str, Any]
    ) -> FileMaterializationInspection:
        try:
            ledger = self._read_ledger(identity)
            epoch = ledger["epoch"] if ledger is not None else None
            ledger_reason = ledger["reason"] if ledger is not None else None
            reference_path = self._reference_path(identity)
            try:
                reference_path.lstat()
            except FileNotFoundError:
                if ledger is not None:
                    return FileMaterializationInspection(
                        "invalidated", reason=ledger_reason
                    )
                return FileMaterializationInspection("missing")
            except OSError as exc:
                raise FileMaterializationError(
                    "could not inspect materialization reference"
                ) from exc
            try:
                raw_reference = reference_path.read_bytes()
            except FileNotFoundError:
                raise FileMaterializationError(
                    "materialization reference disappeared while reading"
                )
            except OSError as exc:
                raise FileMaterializationError(
                    "could not read materialization reference"
                ) from exc

            reference = _strict_json_document(raw_reference, "reference")
            reference_schema = _require_schema(
                reference, {1: _REF_FIELDS, 2: _REF_FIELDS_V2}, "reference"
            )
            materialization_id = reference.get("materialization_id")
            if type(materialization_id) is not str or not _HEX_ID.fullmatch(
                materialization_id
            ):
                raise FileMaterializationError(
                    "reference has an invalid materialization ID"
                )
            reference_epoch = (
                _validate_epoch(reference["epoch"], "reference")
                if reference_schema == 2
                else None
            )
            if reference_epoch != epoch:
                return FileMaterializationInspection(
                    "invalidated", reason=ledger_reason or "invalidated"
                )
            manifest = self._read_manifest(materialization_id)
            manifest_schema = manifest["schema_version"]
            manifest_epoch = manifest["epoch"] if manifest_schema == 2 else None
            if manifest_epoch != reference_epoch:
                raise FileMaterializationError(
                    "reference and manifest epochs do not match"
                )
            stored_identity = _validate_stored_identity(manifest["identity"])
            if _canonical_json(stored_identity) != _canonical_json(identity):
                raise FileMaterializationError(
                    "manifest identity does not match reference"
                )
            if manifest["codec_id"] != self._codec_id:
                raise FileMaterializationError(
                    "materialization codec does not match", reason="incompatible"
                )
            self._read_generation(materialization_id)
            record = _record_from_identity(materialization_id, context)
            return FileMaterializationInspection("available", record=record)
        except FileMaterializationError as exc:
            return FileMaterializationInspection(exc.reason, reason=str(exc))
        except (OSError, KeyError, TypeError, ValueError):
            error = FileMaterializationError("materialization reference is malformed")
            return FileMaterializationInspection(error.reason, reason=str(error))

    def load(self, record: MaterializationRecord) -> Any:
        """Verify an immutable generation and decode its payload."""
        if (
            not isinstance(record, MaterializationRecord)
            or record.committed is not True
        ):
            raise FileMaterializationError(
                "load requires a committed materialization record"
            )
        try:
            identity = _identity_from_record(record)
        except (AttributeError, TypeError, ValueError, FileMaterializationError) as exc:
            raise FileMaterializationError("record identity is malformed") from exc
        materialization_id = record.materialization_id
        if type(materialization_id) is not str or not _HEX_ID.fullmatch(
            materialization_id
        ):
            raise FileMaterializationError("record has an invalid materialization ID")
        ledger = self._read_ledger(identity)
        current_epoch = ledger["epoch"] if ledger is not None else None
        try:
            manifest_only = self._read_manifest(materialization_id)
            manifest_epoch = (
                manifest_only["epoch"] if manifest_only["schema_version"] == 2 else None
            )
            if manifest_epoch != current_epoch:
                raise FileMaterializationError(
                    "materialization record was invalidated",
                    reason="invalidated",
                )
            manifest, payload = self._read_generation(materialization_id)
        except FileMaterializationError:
            raise
        except OSError as exc:
            raise FileMaterializationError(
                "could not read materialization generation"
            ) from exc
        stored_identity = _validate_stored_identity(manifest["identity"])
        if _canonical_json(stored_identity) != _canonical_json(identity):
            raise FileMaterializationError("record identity does not match generation")
        if manifest["codec_id"] != self._codec_id:
            raise FileMaterializationError(
                "materialization codec does not match", reason="incompatible"
            )
        # Codec errors are intentionally preserved as the asset attempt failure.
        return self.codec.decode(payload)

    def commit(
        self, context: MaterializationContext, output: Any
    ) -> MaterializationRecord:
        """Write a complete immutable generation and atomically publish its ref."""
        identity = _identity(context)
        ledger = self._read_ledger(identity)
        epoch = ledger["epoch"] if ledger is not None else None
        payload = self.codec.encode(output)
        if type(payload) is not bytes:
            raise TypeError("materialization codec encode() must return bytes")

        materialization_id = uuid.uuid4().hex
        record = _record_from_identity(materialization_id, context)
        manifest = {
            "schema_version": 2,
            "materialization_id": materialization_id,
            "identity": identity,
            "codec_id": self._codec_id,
            "payload_size": len(payload),
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
            "epoch": epoch,
        }
        objects_path = self.root / "objects"
        refs_path = self.root / "refs"
        generation_temp: Path | None = None
        reference_temp: Path | None = None
        try:
            objects_path.mkdir(parents=True, exist_ok=True)
            refs_path.mkdir(parents=True, exist_ok=True)
            generation_temp = Path(
                tempfile.mkdtemp(prefix=".generation-", dir=objects_path)
            )
            _write_synced(generation_temp / "payload.bin", payload)
            _write_synced(generation_temp / "manifest.json", _canonical_json(manifest))
            generation_path = objects_path / materialization_id
            os.replace(generation_temp, generation_path)
            generation_temp = None

            reference = {
                "schema_version": 2,
                "materialization_id": materialization_id,
                "epoch": epoch,
            }
            reference_fd, reference_name = tempfile.mkstemp(
                prefix=".reference-", dir=refs_path
            )
            reference_temp = Path(reference_name)
            os.close(reference_fd)
            _write_synced(reference_temp, _canonical_json(reference), existing=True)
            os.replace(reference_temp, self._reference_path(identity))
            reference_temp = None
            return record
        except Exception as exc:
            _remove_path(generation_temp)
            _remove_path(reference_temp)
            if isinstance(exc, FileMaterializationError):
                raise
            raise FileMaterializationError("could not publish materialization") from exc

    def invalidate(
        self, context: MaterializationContext, *, reason: str = "explicit"
    ) -> None:
        """Persist a fresh revocation epoch for one exact identity."""
        identity = _identity(context)
        if type(reason) is not str or not reason:
            raise ValueError("reason must be a non-empty string")
        document = {
            "schema_version": 1,
            "identity": identity,
            "epoch": uuid.uuid4().hex,
            "reason": reason,
        }
        invalidations_path = self.root / "invalidations"
        temporary: Path | None = None
        try:
            # Validate an existing ledger before replacing it, failing closed.
            self._read_ledger(identity)
            invalidations_path.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix=".invalidation-", dir=invalidations_path)
            temporary = Path(name)
            os.close(fd)
            _write_synced(temporary, _canonical_json(document), existing=True)
            os.replace(temporary, self._ledger_path(identity))
            temporary = None
        except Exception as exc:
            _remove_path(temporary)
            if isinstance(exc, FileMaterializationError):
                raise
            raise FileMaterializationError(
                "could not publish materialization invalidation"
            ) from exc

    def _reference_path(self, identity: dict[str, Any]) -> Path:
        digest = hashlib.sha256(_canonical_json(identity)).hexdigest()
        return self.root / "refs" / f"{digest}.json"

    def _ledger_path(self, identity: dict[str, Any]) -> Path:
        digest = hashlib.sha256(_canonical_json(identity)).hexdigest()
        return self.root / "invalidations" / f"{digest}.json"

    def _read_ledger(self, identity: dict[str, Any]) -> dict[str, Any] | None:
        # A dangling parent must not erase revocation evidence as a cache miss.
        parent = self.root / "invalidations"
        try:
            parent.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise FileMaterializationError(
                "could not inspect invalidation directory"
            ) from exc
        if not parent.is_dir():
            raise FileMaterializationError("invalidation directory is unreadable")
        path = self._ledger_path(identity)
        try:
            path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise FileMaterializationError(
                "could not read invalidation ledger"
            ) from exc
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise FileMaterializationError(
                "could not read invalidation ledger"
            ) from exc
        ledger = _strict_json_document(raw, "invalidation ledger")
        if set(ledger) != _LEDGER_FIELDS:
            raise FileMaterializationError("invalidation ledger has unexpected fields")
        if type(ledger["schema_version"]) is not int:
            raise FileMaterializationError(
                "invalidation ledger schema_version must be an integer"
            )
        if ledger["schema_version"] != 1:
            raise FileMaterializationError(
                "unsupported invalidation ledger schema version",
                reason="incompatible",
            )
        stored_identity = _validate_stored_identity(ledger["identity"])
        if _canonical_json(stored_identity) != _canonical_json(identity):
            raise FileMaterializationError(
                "invalidation ledger identity does not match"
            )
        if _validate_epoch(ledger["epoch"], "invalidation ledger") is None:
            raise FileMaterializationError("invalidation ledger epoch is missing")
        if type(ledger["reason"]) is not str or not ledger["reason"]:
            raise FileMaterializationError("invalidation ledger reason is malformed")
        return ledger

    def _read_manifest(self, materialization_id: str) -> dict[str, Any]:
        generation = self.root / "objects" / materialization_id
        try:
            manifest = _strict_json_document(
                (generation / "manifest.json").read_bytes(), "manifest"
            )
            return _validate_manifest(manifest, materialization_id)
        except FileMaterializationError:
            raise
        except OSError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise FileMaterializationError("manifest is malformed") from exc

    def _read_generation(self, materialization_id: str) -> tuple[dict[str, Any], bytes]:
        generation = self.root / "objects" / materialization_id
        try:
            manifest = self._read_manifest(materialization_id)
            payload = (generation / "payload.bin").read_bytes()
            if len(payload) != manifest["payload_size"]:
                raise FileMaterializationError("payload size does not match manifest")
            if hashlib.sha256(payload).hexdigest() != manifest["payload_sha256"]:
                raise FileMaterializationError("payload digest does not match manifest")
            return manifest, payload
        except FileMaterializationError:
            raise
        except OSError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise FileMaterializationError("manifest is malformed") from exc


def _identity(context: MaterializationContext) -> dict[str, Any]:
    if not isinstance(context, MaterializationContext):
        raise TypeError("context must be a MaterializationContext")
    try:
        context = MaterializationContext(
            asset_name=context.asset_name,
            version=context.version,
            partition_domain=context.partition_domain,
            partition_key_present=context.partition_key_present,
            partition_key=context.partition_key,
            input_materialization_ids=context.input_materialization_ids,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise FileMaterializationError("materialization context is malformed") from exc
    key = context.partition_key
    if key is None:
        encoded_key: dict[str, Any] = {"type": "none"}
    elif type(key) is str:
        encoded_key = {"type": "str", "value": key}
    elif type(key) is bool:
        encoded_key = {"type": "bool", "value": key}
    elif type(key) is int:
        encoded_key = {"type": "int", "value": str(key)}
    elif type(key) is float and math.isfinite(key):
        encoded_key = {"type": "float", "value": key.hex()}
    elif type(key) is date:
        encoded_key = {"type": "date", "value": key.isoformat()}
    else:
        raise FileMaterializationError(
            "partition key must be an exact str, bool, int, finite float, date, or None"
        )
    return {
        "asset_name": context.asset_name,
        "version": context.version,
        "partition_domain": context.partition_domain,
        "partition_key_present": context.partition_key_present,
        "partition_key": encoded_key,
        "input_materialization_ids": [
            list(pair) for pair in context.input_materialization_ids
        ],
    }


def _identity_from_record(record: MaterializationRecord) -> dict[str, Any]:
    # Reuse the public context validation for defensive checks on manually
    # constructed or mutated records, while preserving exact key type.
    context = MaterializationContext(
        asset_name=record.asset_name,
        version=record.version,
        partition_domain=record.partition_domain,
        partition_key_present=record.partition_key_present,
        partition_key=record.partition_key,
        input_materialization_ids=record.input_materialization_ids,
    )
    return _identity(context)


def _validate_stored_identity(value: Any) -> dict[str, Any]:
    """Validate the manifest's typed identity without Python's bool/int aliasing."""
    if type(value) is not dict or set(value) != {
        "asset_name",
        "version",
        "partition_domain",
        "partition_key_present",
        "partition_key",
        "input_materialization_ids",
    }:
        raise FileMaterializationError("manifest identity has an invalid shape")
    if type(value["partition_key_present"]) is not bool:
        raise FileMaterializationError("manifest identity presence must be a boolean")
    key = value["partition_key"]
    if type(key) is not dict or set(key) not in (
        {"type"},
        {"type", "value"},
    ):
        raise FileMaterializationError("manifest typed partition key is malformed")
    key_type = key["type"]
    if type(key_type) is not str:
        raise FileMaterializationError("manifest partition key type must be a string")
    if key_type == "none":
        if set(key) != {"type"}:
            raise FileMaterializationError("none partition key cannot have a value")
    elif key_type == "str":
        if set(key) != {"type", "value"} or type(key["value"]) is not str:
            raise FileMaterializationError("manifest string partition key is malformed")
    elif key_type == "bool":
        if set(key) != {"type", "value"} or type(key["value"]) is not bool:
            raise FileMaterializationError(
                "manifest boolean partition key is malformed"
            )
    elif key_type == "int":
        if set(key) != {"type", "value"} or type(key["value"]) is not str:
            raise FileMaterializationError(
                "manifest integer partition key is malformed"
            )
        try:
            parsed_int = int(key["value"])
        except ValueError as exc:
            raise FileMaterializationError(
                "manifest integer partition key is malformed"
            ) from exc
        if str(parsed_int) != key["value"]:
            raise FileMaterializationError("manifest integer key is not canonical")
    elif key_type == "float":
        if set(key) != {"type", "value"} or type(key["value"]) is not str:
            raise FileMaterializationError("manifest float partition key is malformed")
        try:
            parsed_float = float.fromhex(key["value"])
        except (OverflowError, ValueError) as exc:
            raise FileMaterializationError(
                "manifest float partition key is malformed"
            ) from exc
        if not math.isfinite(parsed_float) or parsed_float.hex() != key["value"]:
            raise FileMaterializationError(
                "manifest float key is not canonical and finite"
            )
    elif key_type == "date":
        if set(key) != {"type", "value"} or type(key["value"]) is not str:
            raise FileMaterializationError("manifest date partition key is malformed")
        try:
            parsed_date = date.fromisoformat(key["value"])
        except ValueError as exc:
            raise FileMaterializationError(
                "manifest date partition key is malformed"
            ) from exc
        if parsed_date.isoformat() != key["value"]:
            raise FileMaterializationError("manifest date key is not canonical")
    else:
        raise FileMaterializationError("manifest has an unsupported partition key type")

    for field in ("asset_name", "version", "partition_domain"):
        if type(value[field]) is not str or not value[field]:
            raise FileMaterializationError(f"manifest identity {field} is invalid")
    inputs = value["input_materialization_ids"]
    if type(inputs) is not list:
        raise FileMaterializationError(
            "manifest input materialization IDs must be a list"
        )
    seen: set[str] = set()
    for pair in inputs:
        if (
            type(pair) is not list
            or len(pair) != 2
            or type(pair[0]) is not str
            or not pair[0]
            or type(pair[1]) is not str
            or not pair[1]
        ):
            raise FileMaterializationError(
                "manifest input materialization ID pair is malformed"
            )
        if pair[0] in seen:
            raise FileMaterializationError(
                "manifest has duplicate input dependency names"
            )
        seen.add(pair[0])
    # Ensure an absent key is represented consistently and rebuild in the
    # canonical shape used for reference addressing.
    if value["partition_key_present"] == (key_type == "none"):
        raise FileMaterializationError("manifest key type conflicts with key presence")
    return {
        "asset_name": value["asset_name"],
        "version": value["version"],
        "partition_domain": value["partition_domain"],
        "partition_key_present": value["partition_key_present"],
        "partition_key": key,
        "input_materialization_ids": inputs,
    }


def _record_from_identity(
    materialization_id: str, context: MaterializationContext
) -> MaterializationRecord:
    return MaterializationRecord(
        materialization_id=materialization_id,
        asset_name=context.asset_name,
        version=context.version,
        partition_domain=context.partition_domain,
        partition_key_present=context.partition_key_present,
        partition_key=context.partition_key,
        input_materialization_ids=context.input_materialization_ids,
        committed=True,
    )


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _strict_json_document(raw: bytes, label: str) -> dict[str, Any]:
    try:
        parsed = json.loads(
            raw.decode("utf-8"),
            parse_constant=_reject_json_constant,
            object_pairs_hook=_unique_json_object,
        )
    except (
        UnicodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
        RecursionError,
    ) as exc:
        raise FileMaterializationError(f"{label} is not valid strict JSON") from exc
    if type(parsed) is not dict:
        raise FileMaterializationError(f"{label} must be a JSON object")
    return parsed


def _require_schema(
    document: dict[str, Any], schemas: dict[int, set[str]], label: str
) -> int:
    schema_version = document.get("schema_version")
    if type(schema_version) is not int:
        raise FileMaterializationError(f"{label} schema_version must be an integer")
    expected_fields = schemas.get(schema_version)
    if expected_fields is None:
        raise FileMaterializationError(
            f"unsupported {label} schema version", reason="incompatible"
        )
    if set(document) != expected_fields:
        raise FileMaterializationError(f"{label} has unexpected fields")
    return schema_version


def _validate_epoch(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if type(value) is not str or not _HEX_ID.fullmatch(value):
        raise FileMaterializationError(f"{label} epoch is malformed")
    return value


def _validate_manifest(
    manifest: dict[str, Any], materialization_id: str
) -> dict[str, Any]:
    schema_version = _require_schema(
        manifest, {1: _MANIFEST_FIELDS, 2: _MANIFEST_FIELDS_V2}, "manifest"
    )
    if manifest["materialization_id"] != materialization_id:
        raise FileMaterializationError("manifest materialization ID does not match")
    if schema_version == 2:
        _validate_epoch(manifest["epoch"], "manifest")
    if type(manifest["identity"]) is not dict:
        raise FileMaterializationError("manifest identity must be an object")
    if type(manifest["codec_id"]) is not str or not manifest["codec_id"]:
        raise FileMaterializationError("manifest codec_id must be a non-empty string")
    if type(manifest["payload_size"]) is not int or manifest["payload_size"] < 0:
        raise FileMaterializationError("manifest payload_size must be non-negative")
    payload_sha256 = manifest["payload_sha256"]
    if type(payload_sha256) is not str or not _SHA256.fullmatch(payload_sha256):
        raise FileMaterializationError("manifest payload digest is malformed")
    return manifest


def _write_synced(path: Path, data: bytes, *, existing: bool = False) -> None:
    mode = "wb" if existing else "xb"
    with path.open(mode) as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _remove_path(path: Path | None) -> None:
    if path is None:
        return
    try:
        if path.is_dir() and not path.is_symlink():
            for child in path.iterdir():
                _remove_path(child)
            path.rmdir()
        else:
            path.unlink(missing_ok=True)
    except OSError:
        pass
