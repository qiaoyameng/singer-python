# pylint: disable=too-many-lines
"""Flatten configured one-level array-of-object fields into child streams.

A target warehouse that only accepts flat tables cannot store the nested
arrays emitted by many taps. This module sits between a tap and a target and
re-writes the Singer message stream: each configured array-of-object property
of a parent stream is expanded into its own child stream while the parent
stream keeps every other field.

For one parent record the derived messages are written in a fixed order:

  1. the parent RECORD (without the configured array properties),
  2. the child RECORDs of the first configured array in array order,
  3. the child RECORDs of the second configured array in array order, etc.

Only after every derived message has been written is the following STATE
message forwarded. If a write fails the exception propagates immediately, no
STATE is swallowed and the caller must not assume the target committed.
"""
import copy
import json
import sys

import singer.messages as messages
from singer import metadata as metadata_module
from singer.catalog import Catalog, CatalogEntry
from singer.exceptions import (
    ArrayFlattenerConfigurationError,
    ArrayFlattenerSyncError,
)
from singer.utils import should_sync_field

STATUS_MISSING = 'missing'
STATUS_NULL = 'null'
STATUS_EMPTY = 'empty'
STATUS_PRESENT = 'present'

_ARRAY_INDEX_COLUMN = '_sdc_source_array_index'
_STATUS_COLUMN_SUFFIX = '_array_status'


def _status_column(array_path):
    return f'{array_path}{_STATUS_COLUMN_SUFFIX}'


class ArraySpec:
    """Description of one array-of-object property to expand."""

    def __init__(self, array_path, child_stream=None):
        if not isinstance(array_path, str) or not array_path:
            raise ArrayFlattenerConfigurationError(
                f'Each array spec needs a non-empty "array_path", got: {array_path!r}')
        if '.' in array_path:
            raise ArrayFlattenerConfigurationError(
                f'Only top-level array-of-object fields are supported, '
                f'got nested path: {array_path!r}')
        self.array_path = array_path
        self.child_stream = child_stream or array_path

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict):
            raise ArrayFlattenerConfigurationError(
                f'Each array spec must be an object, got: {raw!r}')
        return cls(
            array_path=raw.get('array_path'),
            child_stream=raw.get('child_stream'),
        )


class ParentStreamConfig:
    """Flattening configuration for one parent stream."""

    def __init__(self, stream, key_properties, arrays):
        if not isinstance(stream, str) or not stream:
            raise ArrayFlattenerConfigurationError(
                f'A parent stream needs a non-empty "stream" name, got: {stream!r}')
        if not isinstance(key_properties, list) or not key_properties:
            raise ArrayFlattenerConfigurationError(
                f'Parent stream {stream!r} must declare a non-empty list of '
                f'"key_properties", got: {key_properties!r}')
        for key_property in key_properties:
            if not isinstance(key_property, str) or not key_property:
                raise ArrayFlattenerConfigurationError(
                    f'Parent stream {stream!r} has an invalid key property: {key_property!r}')
        if not isinstance(arrays, list) or not arrays:
            raise ArrayFlattenerConfigurationError(
                f'Parent stream {stream!r} needs at least one array spec, got: {arrays!r}')

        self.stream = stream
        self.key_properties = list(key_properties)
        self.array_specs = []
        seen_paths = set()
        seen_child_streams = set()
        for raw_spec in arrays:
            spec = raw_spec if isinstance(raw_spec, ArraySpec) else ArraySpec.from_dict(raw_spec)
            if spec.array_path in seen_paths:
                raise ArrayFlattenerConfigurationError(
                    f'Array path {spec.array_path!r} is configured more than once '
                    f'for stream {stream!r}')
            if spec.child_stream in seen_child_streams:
                raise ArrayFlattenerConfigurationError(
                    f'Child stream name {spec.child_stream!r} is used more than once '
                    f'for stream {stream!r}')
            seen_paths.add(spec.array_path)
            seen_child_streams.add(spec.child_stream)
            self.array_specs.append(spec)

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict):
            raise ArrayFlattenerConfigurationError(
                f'Each stream config must be an object, got: {raw!r}')
        return cls(
            stream=raw.get('stream'),
            key_properties=raw.get('key_properties'),
            arrays=raw.get('arrays'),
        )


class _ArrayRuntime:
    """Resolved schema-time information for one child stream."""

    def __init__(self, spec, status_column):
        self.spec = spec
        self.status_column = status_column
        self.child_stream = spec.child_stream


def parse_config(raw):
    """Validate the flattener configuration and build ParentStreamConfig objects."""
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, dict):
        raise ArrayFlattenerConfigurationError(
            f'Flattener config must be an object, got: {type(raw).__name__}')
    streams = raw.get('streams')
    if not isinstance(streams, list) or not streams:
        raise ArrayFlattenerConfigurationError(
            'Flattener config needs a non-empty "streams" list')

    configs = []
    seen_streams = set()
    seen_child_streams = set()
    for raw_stream in streams:
        config = ParentStreamConfig.from_dict(raw_stream)
        if config.stream in seen_streams:
            raise ArrayFlattenerConfigurationError(
                f'Parent stream {config.stream!r} is configured more than once')
        seen_streams.add(config.stream)
        for spec in config.array_specs:
            if spec.child_stream in seen_streams or spec.child_stream in seen_child_streams:
                raise ArrayFlattenerConfigurationError(
                    f'Child stream name {spec.child_stream!r} collides with another stream')
            seen_child_streams.add(spec.child_stream)
        configs.append(config)
    return configs


class _StreamRuntime:
    """Per-parent state accumulated while processing the message stream."""

    def __init__(self, config):
        self.config = config
        self.arrays = []
        self.seen_keys = set()


class ArrayFlattener:
    """Rewrite a Singer message stream, expanding configured array fields.

    Parameters
    ----------
    config: dict or str
        Flattener configuration (or a JSON string). See ``parse_config``.
    writer: callable, optional
        Receives every output ``singer.Message``. Defaults to writing JSON
        lines to stdout via ``singer.messages.write_message``. Inject a
        failing callable in tests to verify failure handling.
    catalog: dict or singer.catalog.Catalog, optional
        Input catalog used for field selection. Streams and fields marked
        ``selected: false`` (or ``inclusion: unsupported``) are dropped from
        emitted RECORD payloads. Injected key, index and status columns are
        always retained. Without a catalog every field is retained.
    """

    def __init__(self, config, writer=None, catalog=None):
        self.configs = parse_config(config)
        self._config_by_stream = {config.stream: config for config in self.configs}
        self._writer = writer
        self._runtime = {}
        self._known_streams = set()
        self.catalog = Catalog([])
        self._selection = self._build_selection(catalog)
        self._failed = False

    def _emit(self, message):
        if self._failed:
            raise ArrayFlattenerSyncError(
                'Flattener already failed on a previous write; refusing to emit more messages')
        if self._writer is None:
            messages.write_message(message)
        else:
            try:
                self._writer(message)
            except Exception:
                self._failed = True
                raise

    def _build_selection(self, catalog):
        if catalog is None:
            return {}
        if isinstance(catalog, Catalog):
            entries = catalog.streams
        elif isinstance(catalog, dict):
            entries = Catalog.from_dict(catalog).streams
        else:
            raise ArrayFlattenerConfigurationError(
                'catalog must be a Catalog instance or a catalog dict')

        selection = {}
        for entry in entries:
            fields = {}
            mdata = metadata_module.to_map(entry.metadata or [])
            schema_dict = entry.schema.to_dict() if entry.schema is not None else {}
            for field_name in schema_dict.get('properties', {}):
                field_metadata = mdata.get(('properties', field_name), {})
                fields[field_name] = (
                    field_metadata.get('inclusion', 'available'),
                    field_metadata.get('selected'),
                )
            selection[entry.tap_stream_id or entry.stream] = fields
        return selection

    def _keep_field(self, stream_name, field_name):
        fields = self._selection.get(stream_name)
        if not fields or field_name not in fields:
            return True
        inclusion, selected = fields[field_name]
        return should_sync_field(inclusion, selected, default=True)

    def handle_message(self, message):
        """Process one parsed Singer message and emit its derived messages."""
        if isinstance(message, messages.SchemaMessage):
            self._on_schema(message)
        elif isinstance(message, messages.RecordMessage):
            self._on_record(message)
        elif isinstance(message, (messages.StateMessage, messages.ActivateVersionMessage)):
            self._emit(message)
        elif message is None:
            raise ArrayFlattenerSyncError('Received an unrecognized Singer message')
        else:
            raise ArrayFlattenerSyncError(
                f'Unsupported message type {message.__class__.__name__}')

    def consume(self, message_iterable):
        """Process an iterable of parsed ``singer.Message`` objects."""
        for message in message_iterable:
            self.handle_message(message)

    def consume_lines(self, lines):
        """Process an iterable of raw JSON-line Singer messages."""
        for line in lines:
            if line and line.strip():
                self.handle_message(messages.parse_message(line))

    def _on_schema(self, message):
        stream_name = message.stream
        if stream_name in self._known_streams:
            raise ArrayFlattenerConfigurationError(
                f'Duplicate SCHEMA message for stream {stream_name!r}')

        config = self._config_by_stream.get(stream_name)
        if config is None:
            self._known_streams.add(stream_name)
            self._emit(message)
            return

        key_properties = list(message.key_properties or [])
        if key_properties != config.key_properties:
            raise ArrayFlattenerConfigurationError(
                f'SCHEMA key_properties {key_properties!r} for stream {stream_name!r} do not '
                f'match the configured key_properties {config.key_properties!r}')
        self._validate_parent_schema(message.schema, config)

        runtime = _StreamRuntime(config)
        parent_schema, child_schemas = self._build_schemas(message.schema, config, runtime)

        for child_stream, _, _ in child_schemas:
            if child_stream in self._known_streams:
                raise ArrayFlattenerConfigurationError(
                    f'Child stream {child_stream!r} collides with an existing stream')

        parent_metadata = self._standard_metadata(parent_schema, config.key_properties)
        self._register_catalog_entry(
            stream_name, parent_schema, config.key_properties, parent_metadata)
        self._emit(messages.SchemaMessage(
            stream_name, parent_schema, config.key_properties))

        for child_stream, child_schema, child_keys in child_schemas:
            child_metadata = self._standard_metadata(child_schema, child_keys)
            self._register_catalog_entry(
                child_stream, child_schema, child_keys, child_metadata)
            self._emit(messages.SchemaMessage(child_stream, child_schema, child_keys))

        self._runtime[stream_name] = runtime
        self._known_streams.add(stream_name)

    def _validate_parent_schema(self, schema, config):
        if not isinstance(schema, dict) or not isinstance(schema.get('properties'), dict):
            raise ArrayFlattenerConfigurationError(
                f'SCHEMA for {config.stream!r} must be an object schema with "properties"')
        properties = schema['properties']

        for key_property in config.key_properties:
            if properties.get(key_property) is None:
                raise ArrayFlattenerConfigurationError(
                    f'Declared key property {key_property!r} is missing from the schema of '
                    f'{config.stream!r}')

        array_paths = {spec.array_path for spec in config.array_specs}
        injected_columns = set(config.key_properties) | {_ARRAY_INDEX_COLUMN}
        for spec in config.array_specs:
            array_prop = properties.get(spec.array_path)
            if array_prop is None:
                raise ArrayFlattenerConfigurationError(
                    f'Configured array path {spec.array_path!r} is missing from the schema of '
                    f'{config.stream!r}')
            self._validate_array_property(config.stream, spec, array_prop, injected_columns)

            status_column = _status_column(spec.array_path)
            if status_column in properties:
                raise ArrayFlattenerConfigurationError(
                    f'Injected status column {status_column!r} conflicts with an existing '
                    f'property in {config.stream!r}')
            if status_column in array_paths:
                raise ArrayFlattenerConfigurationError(
                    f'Injected status column {status_column!r} conflicts with configured array '
                    f'path in {config.stream!r}')

    def _validate_array_property(self, stream_name, spec, array_prop, injected_columns):
        declared_types = array_prop.get('type', [])
        if isinstance(declared_types, str):
            declared_types = [declared_types]
        if 'array' not in declared_types:
            raise ArrayFlattenerConfigurationError(
                f'Property {spec.array_path!r} of {stream_name!r} is not declared as an array')
        items = array_prop.get('items')
        if not isinstance(items, dict) or not isinstance(items.get('properties'), dict):
            raise ArrayFlattenerConfigurationError(
                f'Array {spec.array_path!r} of {stream_name!r} must declare object "items" '
                f'with "properties"')
        for column in injected_columns:
            if column in items['properties']:
                raise ArrayFlattenerConfigurationError(
                    f'Injected column {column!r} conflicts with item property '
                    f'{spec.array_path!r}.{column} in stream {stream_name!r}')

    def _build_schemas(self, schema, config, runtime):
        parent_schema = copy.deepcopy(schema)
        parent_properties = parent_schema['properties']
        parent_required = parent_schema.setdefault('required', [])
        child_schemas = []

        for spec in config.array_specs:
            array_prop = parent_properties.pop(spec.array_path)
            if spec.array_path in parent_required:
                parent_required.remove(spec.array_path)

            status_column = _status_column(spec.array_path)
            parent_properties[status_column] = {
                'type': ['null', 'string'],
                'enum': [STATUS_MISSING, STATUS_NULL, STATUS_EMPTY, STATUS_PRESENT],
            }
            if status_column not in parent_required:
                parent_required.append(status_column)

            child_schema = copy.deepcopy(array_prop['items'])
            child_schema.setdefault('type', 'object')
            child_properties = child_schema.setdefault('properties', {})
            for key_property in config.key_properties:
                child_properties[key_property] = copy.deepcopy(parent_properties[key_property])
            child_properties[_ARRAY_INDEX_COLUMN] = {'type': 'integer'}

            child_keys = list(config.key_properties) + [_ARRAY_INDEX_COLUMN]
            child_required = list(child_schema.get('required', []))
            for key in child_keys:
                if key not in child_required:
                    child_required.append(key)
            child_schema['required'] = child_required

            runtime.arrays.append(_ArrayRuntime(spec, status_column))
            child_schemas.append((spec.child_stream, child_schema, child_keys))

        return parent_schema, child_schemas

    def _standard_metadata(self, schema, key_properties):
        mdata = metadata_module.to_map(
            metadata_module.get_standard_metadata(
                schema=schema, key_properties=key_properties))
        metadata_module.write(mdata, (), 'selected', True)
        return metadata_module.to_list(mdata)

    def _register_catalog_entry(self, stream_name, schema, key_properties, mdata):
        from singer.schema import Schema

        self.catalog.streams.append(CatalogEntry(
            tap_stream_id=stream_name,
            stream=stream_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            metadata=mdata,
        ))

    def _on_record(self, message):
        stream_name = message.stream
        config = self._config_by_stream.get(stream_name)
        if config is None:
            self._emit(message)
            return

        runtime = self._runtime.get(stream_name)
        if runtime is None:
            raise ArrayFlattenerSyncError(
                f'RECORD for {stream_name!r} arrived before its SCHEMA message')
        record = message.record
        if not isinstance(record, dict):
            raise ArrayFlattenerSyncError(
                f'RECORD for {stream_name!r} must be an object, got {type(record).__name__}')

        key_values = self._extract_parent_keys(stream_name, config, record, runtime)

        array_paths = {spec.array_path for spec in config.array_specs}
        parent_record = {}
        for name, value in record.items():
            if name in array_paths:
                continue
            if name in config.key_properties or self._keep_field(stream_name, name):
                parent_record[name] = value

        array_data = []
        for array_rt in runtime.arrays:
            values, status = self._inspect_array(stream_name, array_rt, record)
            parent_record[array_rt.status_column] = status
            array_data.append((array_rt, values))

        self._emit(messages.RecordMessage(
            stream_name,
            parent_record,
            version=message.version,
            time_extracted=message.time_extracted,
        ))

        for array_rt, values in array_data:
            if values is not None:
                self._emit_child_records(
                    array_rt, values, config, key_values, stream_name, message)

    def _extract_parent_keys(self, stream_name, config, record, runtime):
        key_values = []
        for key_property in config.key_properties:
            if key_property not in record:
                raise ArrayFlattenerSyncError(
                    f'RECORD for {stream_name!r} is missing declared key property '
                    f'{key_property!r}')
            value = record[key_property]
            if value is None:
                raise ArrayFlattenerSyncError(
                    f'RECORD for {stream_name!r} has a null value for key property '
                    f'{key_property!r}')
            try:
                hash(value)
            except TypeError as exc:
                raise ArrayFlattenerSyncError(
                    f'Key property {key_property!r} of {stream_name!r} must be hashable'
                ) from exc
            key_values.append(value)

        key_tuple = tuple(key_values)
        if key_tuple in runtime.seen_keys:
            raise ArrayFlattenerSyncError(
                f'Duplicate parent key {key_tuple!r} received for stream {stream_name!r}')
        runtime.seen_keys.add(key_tuple)
        return key_values

    def _inspect_array(self, stream_name, array_rt, record):
        path = array_rt.spec.array_path
        if path not in record:
            return None, STATUS_MISSING
        raw_value = record[path]
        if raw_value is None:
            return None, STATUS_NULL
        if not isinstance(raw_value, list):
            raise ArrayFlattenerSyncError(
                f'Property {path!r} of {stream_name!r} must be an array or null, got '
                f'{type(raw_value).__name__}')
        if not raw_value:
            return raw_value, STATUS_EMPTY
        return raw_value, STATUS_PRESENT

    def _emit_child_records(self, array_rt, values, config, key_values,
                            stream_name, parent_message):
        child_stream = array_rt.child_stream
        for index, item in enumerate(values):
            if not isinstance(item, dict):
                raise ArrayFlattenerSyncError(
                    f'Item {index} of array {array_rt.spec.array_path!r} in {stream_name!r} '
                    f'is not an object')
            child_record = {}
            for key_property, value in zip(config.key_properties, key_values):
                child_record[key_property] = value
            for name, value in item.items():
                if self._keep_field(child_stream, name):
                    child_record[name] = value
            child_record[_ARRAY_INDEX_COLUMN] = index
            self._emit(messages.RecordMessage(
                child_stream,
                child_record,
                version=parent_message.version,
                time_extracted=parent_message.time_extracted,
            ))


def flatten_input(config, input_stream=None, output_stream=None, catalog=None):
    """Flatten raw JSON-line Singer messages from ``input_stream``.

    Reads until EOF. Returns nothing; output is written to ``output_stream``
    as JSON lines. A writer failure propagates to the caller and no STATE is
    forwarded past the failed message.
    """
    if input_stream is None:
        input_stream = sys.stdin

    def line_writer(message):
        output_stream.write(messages.format_message(message) + '\n')
        output_stream.flush()

    writer = line_writer if output_stream is not None else None
    flattener = ArrayFlattener(config, writer=writer, catalog=catalog)
    flattener.consume_lines(input_stream)


def main(argv=None):
    """Command line entry point: ``--config CONFIG.json`` plus optional catalog."""
    import argparse
    from singer.utils import load_json

    parser = argparse.ArgumentParser(
        description='Flatten configured array-of-object Singer fields into child streams')
    parser.add_argument('-c', '--config', required=True,
                        help='Path to the flattener configuration JSON file')
    parser.add_argument('--catalog', help='Optional input catalog JSON file for field selection')
    args = parser.parse_args(argv)

    config = load_json(args.config)
    catalog = load_json(args.catalog) if args.catalog else None
    flatten_input(config, sys.stdin, sys.stdout, catalog=catalog)


if __name__ == '__main__':
    main()
