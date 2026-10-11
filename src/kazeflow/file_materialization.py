"""Explicit local file storage for reusable materialized asset outputs.

The adapter stores outputs only when its ``commit`` method is called. A codec
must be selected explicitly so applications decide which values are portable
and how their bytes are produced.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
import uuid
from datetime import date
from pathlib import Path
from typing import Any, Protocol

from kazeflow.materialization import MaterializationContext, MaterializationRecord


class FileMaterializationError(Exception):
    """A stored reference or generation is malformed, incompatible, or damaged."""


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
_MANIFEST_FIELDS = {
    "schema_version",
    "materialization_id",
    "identity",
    "codec_id",
    "payload_size",
    "payload_sha256",
}


class FileMaterialization:
    """Store immutable generations under a local root with atomic references.

    ``root`` is trusted local application storage. A missing root is treated as
    a cache miss by :meth:`resolve`; only :meth:`commit` creates directories.
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
        reference_path = self._reference_path(identity)
        try:
            reference_path.lstat()
        except FileNotFoundError:
            return None
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

        try:
            reference = _strict_json_document(raw_reference, "reference")
            if set(reference) != _REF_FIELDS:
                raise FileMaterializationError("reference has unexpected fields")
            if type(reference["schema_version"]) is not int:
                raise FileMaterializationError(
                    "reference schema_version must be an integer"
                )
            if reference["schema_version"] != 1:
                raise FileMaterializationError("unsupported reference schema version")
            materialization_id = reference["materialization_id"]
            if type(materialization_id) is not str or not _HEX_ID.fullmatch(
                materialization_id
            ):
                raise FileMaterializationError(
                    "reference has an invalid materialization ID"
                )
            manifest, _payload = self._read_generation(materialization_id)
            stored_identity = _validate_stored_identity(manifest["identity"])
            if _canonical_json(stored_identity) != _canonical_json(identity):
                raise FileMaterializationError(
                    "manifest identity does not match reference"
                )
            if manifest["codec_id"] != self._codec_id:
                raise FileMaterializationError("materialization codec does not match")
            record = _record_from_identity(materialization_id, context)
            if not record.committed:
                raise FileMaterializationError("resolved generation is not committed")
            return record
        except FileMaterializationError:
            raise
        except (OSError, KeyError, TypeError, ValueError) as exc:
            raise FileMaterializationError(
                "materialization reference is malformed"
            ) from exc

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
        try:
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
            raise FileMaterializationError("materialization codec does not match")
        # Codec errors are intentionally preserved as the asset attempt failure.
        return self.codec.decode(payload)

    def commit(
        self, context: MaterializationContext, output: Any
    ) -> MaterializationRecord:
        """Write a complete immutable generation and atomically publish its ref."""
        identity = _identity(context)
        payload = self.codec.encode(output)
        if type(payload) is not bytes:
            raise TypeError("materialization codec encode() must return bytes")

        materialization_id = uuid.uuid4().hex
        record = _record_from_identity(materialization_id, context)
        manifest = {
            "schema_version": 1,
            "materialization_id": materialization_id,
            "identity": identity,
            "codec_id": self._codec_id,
            "payload_size": len(payload),
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
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

            reference = {"schema_version": 1, "materialization_id": materialization_id}
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

    def _reference_path(self, identity: dict[str, Any]) -> Path:
        digest = hashlib.sha256(_canonical_json(identity)).hexdigest()
        return self.root / "refs" / f"{digest}.json"

    def _read_generation(self, materialization_id: str) -> tuple[dict[str, Any], bytes]:
        generation = self.root / "objects" / materialization_id
        try:
            manifest = _strict_json_document(
                (generation / "manifest.json").read_bytes(), "manifest"
            )
            if set(manifest) != _MANIFEST_FIELDS:
                raise FileMaterializationError("manifest has unexpected fields")
            if type(manifest["schema_version"]) is not int:
                raise FileMaterializationError(
                    "manifest schema_version must be an integer"
                )
            if manifest["schema_version"] != 1:
                raise FileMaterializationError("unsupported manifest schema version")
            if manifest["materialization_id"] != materialization_id:
                raise FileMaterializationError(
                    "manifest materialization ID does not match"
                )
            if type(manifest["identity"]) is not dict:
                raise FileMaterializationError("manifest identity must be an object")
            if type(manifest["codec_id"]) is not str or not manifest["codec_id"]:
                raise FileMaterializationError(
                    "manifest codec_id must be a non-empty string"
                )
            if (
                type(manifest["payload_size"]) is not int
                or manifest["payload_size"] < 0
            ):
                raise FileMaterializationError(
                    "manifest payload_size must be non-negative"
                )
            payload_sha256 = manifest["payload_sha256"]
            if type(payload_sha256) is not str or not _SHA256.fullmatch(payload_sha256):
                raise FileMaterializationError("manifest payload digest is malformed")
            payload = (generation / "payload.bin").read_bytes()
            if len(payload) != manifest["payload_size"]:
                raise FileMaterializationError("payload size does not match manifest")
            if hashlib.sha256(payload).hexdigest() != payload_sha256:
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
