"""Ordered validation for Singer message streams.

The validator consumes ``singer.messages.Message`` objects (or raw JSON
lines) in the order they were emitted and produces validation events in the
same order.  Each stream keeps its own view of the currently effective
schema and the versions it has declared; a failed message never mutates
that state.

Schema validation is performed with the already-vendored ``jsonschema``
dependency.  Remote ``$ref`` resolution is disabled: schemas that reference
an external URI are rejected up front and the resolver is configured to
fail instead of fetching.
"""

import re
from urllib.parse import urlsplit

from jsonschema import Draft4Validator, RefResolver
from jsonschema.exceptions import RefResolutionError, SchemaError

from singer.exceptions import SingerError
from singer.messages import (
    ActivateVersionMessage,
    RecordMessage,
    SchemaMessage,
    StateMessage,
    parse_message,
)

_REMOTE_SCHEMES = ("http", "https", "ftp", "ftps")


class VersionPolicy:
    """Configurable protocol for the ``version`` field of RECORD messages.

    * ``ALLOW``  - records may carry a version but do not have to.
    * ``REQUIRE`` - every record must carry an integer version.
    * ``FORBID`` - records must not carry a version and ACTIVATE_VERSION
      messages are not permitted for the stream.
    """

    ALLOW = "allow"
    REQUIRE = "require"
    FORBID = "forbid"

    _VALUES = (ALLOW, REQUIRE, FORBID)

    @classmethod
    def validate(cls, policy):
        if policy not in cls._VALUES:
            raise ValueError(
                "version_policy must be one of {}, got {!r}".format(
                    cls._VALUES, policy))
        return policy


class ValidationEvent:
    """Base class for events emitted in input order."""

    def __init__(self, sequence):
        self.sequence = sequence

    def asdict(self):
        result = {"event": self.__class__.__name__}
        for key, value in self.__dict__.items():
            result[key] = value
        return result

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.__dict__ == other.__dict__
        )

    def __repr__(self):
        pairs = ["{}={!r}".format(k, v) for k, v in self.__dict__.items()]
        return "{}({})".format(self.__class__.__name__, ", ".join(pairs))


class SchemaAccepted(ValidationEvent):
    """A valid SCHEMA message became the effective schema of a stream."""

    def __init__(self, sequence, stream, schema_generation):
        super().__init__(sequence)
        self.stream = stream
        self.schema_generation = schema_generation


class RecordAccepted(ValidationEvent):
    """A RECORD message passed validation against the current schema."""

    def __init__(self, sequence, stream, version):
        super().__init__(sequence)
        self.stream = stream
        self.version = version


class VersionActivated(ValidationEvent):
    """An ACTIVATE_VERSION message referenced a declared version."""

    def __init__(self, sequence, stream, version):
        super().__init__(sequence)
        self.stream = stream
        self.version = version


class CheckpointEvent(ValidationEvent):
    """A STATE message with the validator's observations up to this point.

    This is a reporting event only.  The validator never writes to a target,
    so a checkpoint does not claim that any target has committed the data.
    """

    def __init__(self, sequence, value, stream_counts, total_errors):
        super().__init__(sequence)
        self.stream = None
        self.value = value
        self.stream_counts = stream_counts
        self.total_errors = total_errors


class ValidationErrorEvent(ValidationEvent):
    """A validation failure.

    ``stream`` and ``path`` are ``None`` when the failure is not tied to a
    specific stream or field (for example an unparseable input line).
    """

    def __init__(self, sequence, stream, path, code, message):
        super().__init__(sequence)
        self.stream = stream
        self.path = path
        self.code = code
        self.message = message

    def __str__(self):
        return (
            "seq={} stream={!r} path={!r} code={}: {}".format(
                self.sequence, self.stream, self.path, self.code,
                self.message))


class MessageValidationError(SingerError):
    """Raised in stop mode when the first validation error is encountered."""

    def __init__(self, event):
        self.event = event
        super().__init__(str(event))


class _StreamState:
    """Per-stream protocol state. Only mutated by accepted messages."""

    def __init__(self, name):
        self.name = name
        self.schema = None
        self.validator = None
        self.schema_generation = 0
        self.declared_versions = set()
        self.schemas_accepted = 0
        self.records_accepted = 0
        self.records_rejected = 0
        self.activations_accepted = 0
        self.errors = 0

    def snapshot(self):
        return {
            "schema_generation": self.schema_generation,
            "has_schema": self.validator is not None,
            "schemas_accepted": self.schemas_accepted,
            "records_accepted": self.records_accepted,
            "records_rejected": self.records_rejected,
            "versions_declared": sorted(self.declared_versions),
            "activations_accepted": self.activations_accepted,
            "errors": self.errors,
        }


def _reject_remote(uri):
    raise RefResolutionError(
        "remote $ref resolution is disabled, refusing to fetch: {}".format(uri))


def _find_remote_ref(node):
    """Return the first remote ``$ref`` found in a schema, else ``None``."""
    stack = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            ref = current.get("$ref")
            if isinstance(ref, str) and urlsplit(ref).scheme:
                return ref
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return None


def _format_field_path(absolute_path):
    parts = []
    for piece in absolute_path:
        if isinstance(piece, int):
            parts.append("[{}]".format(piece))
        else:
            parts.append(str(piece))
    if not parts:
        return None
    path = parts[0]
    for piece in parts[1:]:
        if piece.startswith("["):
            path += piece
        else:
            path += "." + piece
    return path


def _error_field_path(error):
    """Human-readable field path, including missing required fields."""
    path = _format_field_path(error.absolute_path)
    if error.validator == "required":
        match = re.match(r"^'([^']+)' is a required property$",
                         error.message)
        missing = match.group(1) if match else None
        if missing is None:
            return path
        path = missing if path is None else "{}.{}".format(path, missing)
    return path


def _is_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


class MessageStreamValidator:
    """Validate a Singer message stream and yield ordered events.

    Arguments:

        version_policy: Default protocol for the RECORD ``version`` field.
        stream_version_policies: Optional per-stream overrides keyed by
            stream name.
        collect_errors: When ``True`` (the default) validation failures are
            yielded as :class:`ValidationErrorEvent` objects and stream
            protocol state is left untouched by the failed message. When
            ``False`` the first failure raises
            :class:`MessageValidationError`.
    """

    def __init__(self,
                 version_policy=VersionPolicy.ALLOW,
                 stream_version_policies=None,
                 collect_errors=True):
        self._default_policy = VersionPolicy.validate(version_policy)
        self._stream_policies = {
            name: VersionPolicy.validate(policy)
            for name, policy in (stream_version_policies or {}).items()
        }
        self._collect_errors = collect_errors
        self._streams = {}
        self._sequence = 0
        self._total_errors = 0

    @property
    def total_errors(self):
        return self._total_errors

    def validate(self, messages):
        """Yield one event per outcome, preserving input order."""
        for message in messages:
            yield from self.feed(message)

    def feed(self, message):
        """Feed a single ``Message`` object or JSON line."""
        if isinstance(message, str):
            line = message.strip()
            if not line:
                return
            self._sequence += 1
            sequence = self._sequence
            try:
                parsed = parse_message(line)
            except Exception as exc:
                yield self._failure(
                    sequence, None, None, "invalid_message", str(exc))
                return
            if parsed is None:
                yield self._failure(
                    sequence, None, None, "unknown_message_type",
                    "line does not contain a known Singer message type")
                return
            message = parsed
        else:
            self._sequence += 1
            sequence = self._sequence

        if isinstance(message, SchemaMessage):
            yield from self._handle_schema(sequence, message)
        elif isinstance(message, RecordMessage):
            yield from self._handle_record(sequence, message)
        elif isinstance(message, ActivateVersionMessage):
            yield from self._handle_activation(sequence, message)
        elif isinstance(message, StateMessage):
            yield self._checkpoint(sequence, message)
        else:
            yield self._failure(
                sequence, None, None, "unknown_message_type",
                "unsupported message object: {!r}".format(message))

    def _stream(self, name):
        state = self._streams.get(name)
        if state is None:
            state = _StreamState(name)
            self._streams[name] = state
        return state

    def _policy(self, name):
        return self._stream_policies.get(name, self._default_policy)

    def _failure(self, sequence, stream, path, code, message):
        event = ValidationErrorEvent(sequence, stream, path, code, message)
        if not self._collect_errors:
            raise MessageValidationError(event)
        self._total_errors += 1
        if stream is not None:
            self._stream(stream).errors += 1
        return event

    def _handle_schema(self, sequence, message):
        state = self._stream(message.stream)
        schema = message.schema
        if not isinstance(schema, dict):
            yield self._failure(
                sequence, message.stream, None, "invalid_schema",
                "SCHEMA 'schema' must be a JSON object")
            return

        remote_ref = _find_remote_ref(schema)
        if remote_ref is not None:
            yield self._failure(
                sequence, message.stream, None, "remote_reference",
                "remote $ref is not allowed: {}".format(remote_ref))
            return

        try:
            Draft4Validator.check_schema(schema)
            resolver = RefResolver(
                "",
                schema,
                handlers={scheme: _reject_remote
                          for scheme in _REMOTE_SCHEMES})
            validator = Draft4Validator(schema, resolver=resolver)
        except SchemaError as exc:
            yield self._failure(
                sequence, message.stream,
                _format_field_path(exc.absolute_path),
                "invalid_schema", exc.message)
            return

        state.schema = schema
        state.validator = validator
        state.schema_generation += 1
        state.schemas_accepted += 1
        yield SchemaAccepted(
            sequence, message.stream, state.schema_generation)

    def _handle_record(self, sequence, message):
        state = self._stream(message.stream)
        failures = 0

        if state.validator is None:
            failures += 1
            yield self._failure(
                sequence, message.stream, None, "missing_schema",
                "RECORD arrived before any SCHEMA for stream {!r}".format(
                    message.stream))

        policy = self._policy(message.stream)
        if message.version is not None:
            if not _is_integer(message.version):
                failures += 1
                yield self._failure(
                    sequence, message.stream, "version",
                    "invalid_version_type",
                    "record version must be an integer, got {!r}".format(
                        message.version))
            elif policy == VersionPolicy.FORBID:
                failures += 1
                yield self._failure(
                    sequence, message.stream, "version", "version_forbidden",
                    "stream is configured to forbid record versions")
        elif policy == VersionPolicy.REQUIRE:
            failures += 1
            yield self._failure(
                sequence, message.stream, "version", "version_required",
                "stream requires every record to carry a version")

        if state.validator is not None:
            try:
                schema_errors = sorted(
                    state.validator.iter_errors(message.record),
                    key=lambda error: list(error.absolute_path))
            except RefResolutionError as exc:
                failures += 1
                yield self._failure(
                    sequence, message.stream, None,
                    "remote_reference", str(exc))
            else:
                for error in schema_errors:
                    failures += 1
                    yield self._failure(
                        sequence, message.stream,
                        _error_field_path(error),
                        "schema_violation", error.message)

        if failures:
            state.records_rejected += 1
            return

        state.records_accepted += 1
        if message.version is not None:
            state.declared_versions.add(message.version)
        yield RecordAccepted(
            sequence, message.stream, message.version)

    def _handle_activation(self, sequence, message):
        state = self._stream(message.stream)
        policy = self._policy(message.stream)

        if policy == VersionPolicy.FORBID:
            yield self._failure(
                sequence, message.stream, "version", "version_forbidden",
                "stream is configured to forbid version activation")
            return
        if state.validator is None:
            yield self._failure(
                sequence, message.stream, None, "missing_schema",
                "ACTIVATE_VERSION arrived before any SCHEMA for stream "
                "{!r}".format(message.stream))
            return
        if not _is_integer(message.version):
            yield self._failure(
                sequence, message.stream, "version",
                "invalid_version_type",
                "activate version must be an integer, got {!r}".format(
                    message.version))
            return
        if message.version not in state.declared_versions:
            yield self._failure(
                sequence, message.stream, "version", "undeclared_version",
                "version {} has not been declared by a RECORD for stream "
                "{!r}".format(message.version, message.stream))
            return

        state.activations_accepted += 1
        yield VersionActivated(
            sequence, message.stream, message.version)

    def _checkpoint(self, sequence, message):
        counts = {
            name: state.snapshot()
            for name, state in self._streams.items()
        }
        return CheckpointEvent(
            sequence, message.value, counts, self._total_errors)


def validate_message_stream(messages,
                            version_policy=VersionPolicy.ALLOW,
                            stream_version_policies=None,
                            collect_errors=True):
    """Convenience wrapper returning all validation events as a list."""
    validator = MessageStreamValidator(
        version_policy=version_policy,
        stream_version_policies=stream_version_policies,
        collect_errors=collect_errors)
    return list(validator.validate(messages))
