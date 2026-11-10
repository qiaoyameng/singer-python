import io
import json
import unittest
from unittest import mock

import singer
from singer import ArrayFlattener
from singer import metadata as metadata_module
from singer.array_flattener import (
    STATUS_EMPTY,
    STATUS_MISSING,
    STATUS_NULL,
    STATUS_PRESENT,
    flatten_input,
    parse_config,
)
from singer.exceptions import (
    ArrayFlattenerConfigurationError,
    ArrayFlattenerSyncError,
)
from singer.messages import (
    ActivateVersionMessage,
    RecordMessage,
    SchemaMessage,
    StateMessage,
)


def parent_schema():
    return {
        'type': 'object',
        'additionalProperties': False,
        'required': ['id', 'tags', 'emails'],
        'properties': {
            'id': {'type': 'integer'},
            'name': {'type': ['null', 'string']},
            'tags': {
                'type': ['null', 'array'],
                'items': {
                    'type': 'object',
                    'properties': {
                        'label': {'type': 'string'},
                        'weight': {'type': 'number'},
                    },
                },
            },
            'emails': {
                'type': ['null', 'array'],
                'items': {
                    'type': 'object',
                    'properties': {
                        'address': {'type': 'string'},
                        'primary': {'type': 'boolean'},
                    },
                },
            },
        },
    }


def two_array_config(child_stream=None):
    email_spec = {'array_path': 'emails'}
    if child_stream:
        email_spec['child_stream'] = child_stream
    return {
        'streams': [{
            'stream': 'users',
            'key_properties': ['id'],
            'arrays': [
                {'array_path': 'tags'},
                email_spec,
            ],
        }]
    }


class CollectingFlattener(ArrayFlattener):
    def __init__(self, config, catalog=None, writer=None):
        self.messages = []
        super().__init__(config, writer=writer or self.messages.append, catalog=catalog)

    def lines(self):
        return [singer.format_message(message) for message in self.messages]

    def by_type(self, message_type):
        return [m for m in self.messages if m.asdict()['type'] == message_type]


class TestConfigParsing(unittest.TestCase):
    def test_rejects_empty_config(self):
        with self.assertRaises(ArrayFlattenerConfigurationError):
            parse_config({})

    def test_rejects_missing_key_properties(self):
        config = {'streams': [{'stream': 'users', 'arrays': [{'array_path': 'tags'}]}]}
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'key_properties'):
            parse_config(config)

    def test_rejects_empty_key_properties(self):
        config = {
            'streams': [{
                'stream': 'users',
                'key_properties': [],
                'arrays': [{'array_path': 'tags'}],
            }]
        }
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'key_properties'):
            parse_config(config)

    def test_rejects_nested_array_path(self):
        config = {
            'streams': [{
                'stream': 'users',
                'key_properties': ['id'],
                'arrays': [{'array_path': 'a.b'}],
            }]
        }
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'top-level'):
            parse_config(config)

    def test_rejects_duplicate_parent_streams(self):
        config = {
            'streams': [
                {
                    'stream': 'users',
                    'key_properties': ['id'],
                    'arrays': [{'array_path': 'tags'}],
                },
                {
                    'stream': 'users',
                    'key_properties': ['id'],
                    'arrays': [{'array_path': 'emails'}],
                },
            ]
        }
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'more than once'):
            parse_config(config)

    def test_rejects_child_stream_name_collision(self):
        config = {
            'streams': [{
                'stream': 'users',
                'key_properties': ['id'],
                'arrays': [
                    {'array_path': 'tags', 'child_stream': 'emails'},
                    {'array_path': 'emails'},
                ],
            }]
        }
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, '(collides|more than once)'):
            parse_config(config)

    def test_default_child_stream_name_is_array_path(self):
        configs = parse_config(two_array_config())
        self.assertEqual(
            [spec.child_stream for spec in configs[0].array_specs],
            ['tags', 'emails'])


class TestSchemaGeneration(unittest.TestCase):
    def _flatten_with_schema(self, config=None, schema=None):
        flat = CollectingFlattener(config or two_array_config())
        flat.handle_message(SchemaMessage(
            stream='users',
            schema=schema or parent_schema(),
            key_properties=['id']))
        return flat

    def test_parent_schema_removes_arrays_and_adds_status_columns(self):
        flat = self._flatten_with_schema()
        parent = flat.messages[0]
        properties = parent.schema['properties']
        self.assertNotIn('tags', properties)
        self.assertNotIn('emails', properties)
        self.assertIn('id', properties)
        self.assertIn('name', properties)
        self.assertEqual(
            properties['tags_array_status']['enum'],
            [STATUS_MISSING, STATUS_NULL, STATUS_EMPTY, STATUS_PRESENT])
        self.assertEqual(
            properties['emails_array_status']['enum'],
            [STATUS_MISSING, STATUS_NULL, STATUS_EMPTY, STATUS_PRESENT])
        self.assertNotIn('tags', parent.schema['required'])
        self.assertNotIn('emails', parent.schema['required'])
        for column in ('tags_array_status', 'emails_array_status'):
            self.assertIn(column, parent.schema['required'])

    def test_child_schemas_carry_typed_parent_keys_and_index(self):
        flat = self._flatten_with_schema()
        tags_schema, emails_schema = flat.messages[1], flat.messages[2]
        self.assertEqual(tags_schema.stream, 'tags')
        self.assertEqual(
            tags_schema.key_properties, ['id', '_sdc_source_array_index'])
        self.assertEqual(
            tags_schema.schema['properties']['id'], {'type': 'integer'})
        self.assertEqual(
            tags_schema.schema['properties']['_sdc_source_array_index'],
            {'type': 'integer'})
        self.assertEqual(
            tags_schema.schema['required'],
            ['id', '_sdc_source_array_index'])
        self.assertEqual(
            emails_schema.schema['properties']['id'], {'type': 'integer'})
        self.assertEqual(emails_schema.key_properties, ['id', '_sdc_source_array_index'])

    def test_emitted_schema_order_is_parent_then_children_in_config_order(self):
        flat = self._flatten_with_schema()
        self.assertEqual(
            [m.stream for m in flat.by_type('SCHEMA')],
            ['users', 'tags', 'emails'])

    def test_catalog_contains_parent_and_children_with_selection_metadata(self):
        flat = self._flatten_with_schema()
        catalog_dict = flat.catalog.to_dict()
        self.assertEqual(
            [entry['tap_stream_id'] for entry in catalog_dict['streams']],
            ['users', 'tags', 'emails'])

        tags_entry = catalog_dict['streams'][1]
        mdata = metadata_module.to_map(tags_entry['metadata'])
        self.assertEqual(mdata[()]['selected'], True)
        self.assertEqual(mdata[()]['table-key-properties'],
                         ['id', '_sdc_source_array_index'])
        self.assertEqual(mdata[('properties', 'id')]['inclusion'], 'automatic')
        self.assertEqual(
            mdata[('properties', '_sdc_source_array_index')]['inclusion'],
            'automatic')
        self.assertEqual(mdata[('properties', 'label')]['inclusion'], 'available')

        parent_entry = catalog_dict['streams'][0]
        parent_mdata = metadata_module.to_map(parent_entry['metadata'])
        self.assertEqual(parent_mdata[()]['selected'], True)
        self.assertEqual(parent_mdata[('properties', 'id')]['inclusion'], 'automatic')

    def test_rejects_schema_without_declared_key_property(self):
        schema = parent_schema()
        del schema['properties']['id']
        flat = CollectingFlattener(two_array_config())
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'key property'):
            flat.handle_message(SchemaMessage(
                stream='users', schema=schema, key_properties=['id']))

    def test_rejects_schema_whose_key_properties_differ_from_config(self):
        flat = CollectingFlattener(two_array_config())
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'key_properties'):
            flat.handle_message(SchemaMessage(
                stream='users', schema=parent_schema(), key_properties=['name']))

    def test_rejects_missing_array_property_in_schema(self):
        schema = parent_schema()
        del schema['properties']['tags']
        flat = CollectingFlattener(two_array_config())
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'tags'):
            flat.handle_message(SchemaMessage(
                stream='users', schema=schema, key_properties=['id']))

    def test_rejects_non_array_property(self):
        schema = parent_schema()
        schema['properties']['tags'] = {'type': 'string'}
        flat = CollectingFlattener(two_array_config())
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'not declared as an array'):
            flat.handle_message(SchemaMessage(
                stream='users', schema=schema, key_properties=['id']))

    def test_rejects_array_without_object_items(self):
        schema = parent_schema()
        schema['properties']['tags'] = {'type': 'array', 'items': {'type': 'string'}}
        flat = CollectingFlattener(two_array_config())
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'object "items"'):
            flat.handle_message(SchemaMessage(
                stream='users', schema=schema, key_properties=['id']))

    def test_rejects_injected_column_conflict_in_array_items(self):
        schema = parent_schema()
        schema['properties']['tags']['items']['properties']['_sdc_source_array_index'] = {
            'type': 'integer'}
        flat = CollectingFlattener(two_array_config())
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'Injected column'):
            flat.handle_message(SchemaMessage(
                stream='users', schema=schema, key_properties=['id']))

    def test_rejects_parent_key_conflict_in_array_items(self):
        schema = parent_schema()
        schema['properties']['tags']['items']['properties']['id'] = {'type': 'string'}
        flat = CollectingFlattener(two_array_config())
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'Injected column'):
            flat.handle_message(SchemaMessage(
                stream='users', schema=schema, key_properties=['id']))

    def test_rejects_status_column_conflict_on_parent(self):
        schema = parent_schema()
        schema['properties']['tags_array_status'] = {'type': 'string'}
        flat = CollectingFlattener(two_array_config())
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'status column'):
            flat.handle_message(SchemaMessage(
                stream='users', schema=schema, key_properties=['id']))

    def test_rejects_duplicate_schema_message(self):
        flat = self._flatten_with_schema()
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'Duplicate SCHEMA'):
            flat.handle_message(SchemaMessage(
                stream='users', schema=parent_schema(), key_properties=['id']))

    def test_unconfigured_streams_pass_through(self):
        flat = self._flatten_with_schema()
        other = SchemaMessage(
            stream='other', schema={'type': 'object', 'properties': {}}, key_properties=[])
        flat.handle_message(other)
        self.assertIn(other, flat.messages)
        with self.assertRaisesRegex(ArrayFlattenerConfigurationError, 'Duplicate SCHEMA'):
            flat.handle_message(other)


class TestRecordExpansion(unittest.TestCase):
    def setUp(self):
        self.flat = CollectingFlattener(two_array_config())
        self.flat.handle_message(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']))
        self.flat.messages.clear()

    def test_two_arrays_expand_independently_without_cartesian_product(self):
        record = {
            'id': 1,
            'name': 'ada',
            'tags': [{'label': 'a'}, {'label': 'b'}, {'label': 'a'}],
            'emails': [{'address': 'a@x'}, {'address': 'b@x'}],
        }
        self.flat.handle_message(RecordMessage('users', record))

        records = self.flat.by_type('RECORD')
        self.assertEqual([m.stream for m in records],
                         ['users', 'tags', 'tags', 'tags', 'emails', 'emails'])

        tag_records = [m.record for m in records if m.stream == 'tags']
        self.assertEqual(
            [r['_sdc_source_array_index'] for r in tag_records], [0, 1, 2])
        self.assertEqual([r['label'] for r in tag_records], ['a', 'b', 'a'])
        for child in tag_records:
            self.assertEqual(child['id'], 1)
            self.assertNotIn('address', child)

        email_records = [m.record for m in records if m.stream == 'emails']
        self.assertEqual(
            [r['_sdc_source_array_index'] for r in email_records], [0, 1])
        for child in email_records:
            self.assertEqual(child['id'], 1)
            self.assertNotIn('label', child)

        parent = records[0].record
        self.assertNotIn('tags', parent)
        self.assertNotIn('emails', parent)
        self.assertEqual(parent['tags_array_status'], STATUS_PRESENT)
        self.assertEqual(parent['emails_array_status'], STATUS_PRESENT)
        self.assertEqual(parent['name'], 'ada')

    def test_duplicate_array_elements_are_preserved(self):
        duplicate = {'label': 'same', 'weight': 1.5}
        self.flat.handle_message(RecordMessage('users', {
            'id': 7,
            'tags': [duplicate, duplicate, dict(duplicate)],
        }))
        tag_records = [m.record for m in self.flat.by_type('RECORD') if m.stream == 'tags']
        self.assertEqual(len(tag_records), 3)
        self.assertEqual(
            [r['_sdc_source_array_index'] for r in tag_records], [0, 1, 2])

    def test_missing_null_and_empty_array_statuses_are_distinguished(self):
        self.flat.handle_message(RecordMessage('users', {
            'id': 1,
            # tags missing entirely
            'emails': None,
        }))
        first_parent = self.flat.by_type('RECORD')[0].record
        self.assertEqual(first_parent['tags_array_status'], STATUS_MISSING)
        self.assertEqual(first_parent['emails_array_status'], STATUS_NULL)
        self.assertEqual(
            [m.stream for m in self.flat.by_type('RECORD')], ['users'])

        self.flat.handle_message(RecordMessage('users', {
            'id': 2,
            'tags': [],
            'emails': None,
        }))
        second_parent = self.flat.by_type('RECORD')[1].record
        self.assertEqual(second_parent['tags_array_status'], STATUS_EMPTY)
        self.assertEqual(second_parent['emails_array_status'], STATUS_NULL)

    def test_child_records_carry_typed_parent_key_value(self):
        self.flat.handle_message(RecordMessage('users', {
            'id': 42,
            'tags': [{'label': 'x'}],
        }))
        child = [m for m in self.flat.by_type('RECORD') if m.stream == 'tags'][0]
        self.assertEqual(child.record['id'], 42)
        self.assertIsInstance(child.record['id'], int)

    def test_version_and_time_extracted_propagate_to_children(self):
        import dateutil.parser
        extracted = dateutil.parser.parse('2020-01-01T00:00:00Z')
        self.flat.handle_message(RecordMessage(
            'users',
            {'id': 1, 'tags': [{'label': 'a'}]},
            version=3,
            time_extracted=extracted))
        for message in self.flat.by_type('RECORD'):
            self.assertEqual(message.version, 3)
            self.assertEqual(message.time_extracted, extracted)

    def test_duplicate_parent_key_is_rejected_before_emitting(self):
        self.flat.handle_message(RecordMessage('users', {
            'id': 1, 'tags': [{'label': 'a'}]}))
        before = len(self.flat.messages)
        with self.assertRaisesRegex(ArrayFlattenerSyncError, 'Duplicate parent key'):
            self.flat.handle_message(RecordMessage('users', {
                'id': 1, 'tags': [{'label': 'b'}]}))
        self.assertEqual(len(self.flat.messages), before)

    def test_composite_parent_key_duplicate_detection(self):
        config = {
            'streams': [{
                'stream': 'users',
                'key_properties': ['id', 'region'],
                'arrays': [{'array_path': 'tags'}],
            }]
        }
        schema = copy_schema_with_region()
        flat = CollectingFlattener(config)
        flat.handle_message(SchemaMessage(
            stream='users', schema=schema, key_properties=['id', 'region']))
        flat.handle_message(RecordMessage('users', {
            'id': 1, 'region': 'eu', 'tags': []}))
        flat.handle_message(RecordMessage('users', {
            'id': 1, 'region': 'us', 'tags': []}))
        with self.assertRaisesRegex(ArrayFlattenerSyncError, 'Duplicate parent key'):
            flat.handle_message(RecordMessage('users', {
                'id': 1, 'region': 'eu', 'tags': []}))

    def test_missing_and_null_parent_key_are_rejected(self):
        with self.assertRaisesRegex(ArrayFlattenerSyncError, 'missing declared key'):
            self.flat.handle_message(RecordMessage('users', {'tags': []}))
        with self.assertRaisesRegex(ArrayFlattenerSyncError, 'null value for key'):
            self.flat.handle_message(RecordMessage('users', {'id': None, 'tags': []}))

    def test_record_before_schema_is_rejected(self):
        flat = CollectingFlattener(two_array_config())
        with self.assertRaisesRegex(ArrayFlattenerSyncError, 'before its SCHEMA'):
            flat.handle_message(RecordMessage('users', {'id': 1}))

    def test_wrong_runtime_type_for_array_is_rejected(self):
        with self.assertRaisesRegex(ArrayFlattenerSyncError, 'must be an array or null'):
            self.flat.handle_message(RecordMessage('users', {
                'id': 1, 'tags': {'unexpected': 'dict'}}))

    def test_non_object_array_item_is_rejected(self):
        with self.assertRaisesRegex(ArrayFlattenerSyncError, 'is not an object'):
            self.flat.handle_message(RecordMessage('users', {
                'id': 1, 'tags': ['not-an-object']}))

    def test_unconfigured_stream_records_pass_through(self):
        record = RecordMessage('other', {'anything': True})
        self.flat.handle_message(record)
        self.assertEqual(self.flat.messages, [record])


def copy_schema_with_region():
    schema = parent_schema()
    schema['properties']['region'] = {'type': 'string'}
    del schema['properties']['emails']
    schema['required'].remove('emails')
    return schema


class TestStateOrderingAndFailures(unittest.TestCase):
    def _feed_two_records_and_state(self, flat):
        flat.handle_message(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']))
        flat.handle_message(RecordMessage('users', {
            'id': 1,
            'tags': [{'label': 'a'}, {'label': 'b'}],
            'emails': [{'address': 'a@x'}],
        }))
        flat.handle_message(RecordMessage('users', {
            'id': 2,
            'tags': [],
            'emails': None,
        }))
        flat.handle_message(StateMessage({'bookmark': 2}))

    def test_state_is_forwarded_only_after_all_derived_records(self):
        flat = CollectingFlattener(two_array_config())
        self._feed_two_records_and_state(flat)
        types = [m.asdict()['type'] for m in flat.messages]
        state_indexes = [i for i, t in enumerate(types) if t == 'STATE']
        self.assertEqual(len(state_indexes), 1)
        self.assertEqual(state_indexes[0], len(types) - 1)
        self.assertEqual(flat.messages[-1], StateMessage({'bookmark': 2}))

    def test_activate_version_passes_through(self):
        flat = CollectingFlattener(two_array_config())
        flat.handle_message(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']))
        message = ActivateVersionMessage(stream='users', version=9)
        flat.handle_message(message)
        self.assertEqual(flat.messages[-1], message)

    def test_writer_failure_stops_processing_and_does_not_swallow_state(self):
        class FailingWriter:
            def __init__(self):
                self.calls = 0

            def __call__(self, message):
                self.calls += 1
                if self.calls == 5:
                    raise IOError('simulated target outage')

        writer = FailingWriter()
        flat = ArrayFlattener(two_array_config(), writer=writer)
        flat.handle_message(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']))

        with self.assertRaisesRegex(IOError, 'simulated target outage'):
            flat.handle_message(RecordMessage('users', {
                'id': 1,
                'tags': [{'label': 'a'}, {'label': 'b'}],
                'emails': [{'address': 'a@x'}],
            }))

        state = StateMessage({'bookmark': 1})
        with self.assertRaisesRegex(ArrayFlattenerSyncError, 'already failed'):
            flat.handle_message(state)
        self.assertEqual(writer.calls, 5)

    def test_state_after_failed_record_is_never_emitted(self):
        class FailOnce:
            def __init__(self):
                self.emitted = []
                self.failed = False

            def __call__(self, message):
                if (not self.failed
                        and message.asdict().get('type') == 'RECORD'
                        and getattr(message, 'stream', None) == 'emails'):
                    self.failed = True
                    raise IOError('boom')
                self.emitted.append(message)

        writer = FailOnce()
        flat = ArrayFlattener(two_array_config(), writer=writer)
        flat.handle_message(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']))
        with self.assertRaises(IOError):
            flat.handle_message(RecordMessage('users', {
                'id': 1,
                'tags': [{'label': 'a'}],
                'emails': [{'address': 'a@x'}],
            }))
        with self.assertRaises(ArrayFlattenerSyncError):
            flat.handle_message(StateMessage({'bookmark': 1}))
        self.assertFalse(
            any(isinstance(m, StateMessage) for m in writer.emitted))

    def test_same_input_rerun_produces_byte_identical_output(self):
        def run():
            flat = CollectingFlattener(two_array_config())
            self._feed_two_records_and_state(flat)
            return '\n'.join(flat.lines())

        self.assertEqual(run(), run())

    def test_flatten_input_with_stringio_writer_round_trip(self):
        schema_line = json.dumps(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']).asdict())
        record_line = json.dumps(RecordMessage('users', {
            'id': 1,
            'tags': [{'label': 'a'}, {'label': 'a'}],
            'emails': None,
        }).asdict())
        state_line = json.dumps(StateMessage({'bookmark': 1}).asdict())

        sink = io.StringIO()
        flatten_input(
            two_array_config(),
            input_stream=io.StringIO(
                '\n'.join([schema_line, record_line, state_line]) + '\n'),
            output_stream=sink)

        lines = [json.loads(line) for line in sink.getvalue().splitlines()]
        self.assertEqual([line['type'] for line in lines],
                         ['SCHEMA', 'SCHEMA', 'SCHEMA', 'RECORD', 'RECORD', 'RECORD', 'STATE'])
        streams = [line.get('stream') for line in lines if line['type'] == 'SCHEMA']
        self.assertEqual(streams, ['users', 'tags', 'emails'])
        state_messages = [line for line in lines if line['type'] == 'STATE']
        self.assertEqual(state_messages, [{'type': 'STATE', 'value': {'bookmark': 1}}])

    def test_flatten_input_propagates_failing_writer(self):
        class BrokenStringIO(io.StringIO):
            def write(self, data):
                raise IOError('disk full')

        schema_line = json.dumps(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']).asdict())
        with self.assertRaisesRegex(IOError, 'disk full'):
            flatten_input(
                two_array_config(),
                input_stream=io.StringIO(schema_line + '\n'),
                output_stream=BrokenStringIO())


class TestFieldSelection(unittest.TestCase):
    def setUp(self):
        self.catalog = {
            'streams': [
                {
                    'tap_stream_id': 'users',
                    'stream': 'users',
                    'schema': parent_schema(),
                    'metadata': [
                        {'breadcrumb': (), 'metadata': {'selected': True}},
                        {'breadcrumb': ('properties', 'id'),
                         'metadata': {'inclusion': 'automatic'}},
                        {'breadcrumb': ('properties', 'name'),
                         'metadata': {'inclusion': 'available', 'selected': False}},
                        {'breadcrumb': ('properties', 'tags'),
                         'metadata': {'inclusion': 'available', 'selected': True}},
                        {'breadcrumb': ('properties', 'emails'),
                         'metadata': {'inclusion': 'available', 'selected': True}},
                    ],
                },
                {
                    'tap_stream_id': 'tags',
                    'stream': 'tags',
                    'schema': {
                        'type': 'object',
                        'properties': {
                            'label': {'type': 'string'},
                            'weight': {'type': 'number'},
                            'id': {'type': 'integer'},
                            '_sdc_source_array_index': {'type': 'integer'},
                        },
                    },
                    'metadata': [
                        {'breadcrumb': (), 'metadata': {'selected': True}},
                        {'breadcrumb': ('properties', 'label'),
                         'metadata': {'inclusion': 'available', 'selected': True}},
                        {'breadcrumb': ('properties', 'weight'),
                         'metadata': {'inclusion': 'available', 'selected': False}},
                    ],
                },
            ]
        }

    def test_deselected_fields_are_dropped_but_injected_columns_remain(self):
        flat = CollectingFlattener(two_array_config(), catalog=self.catalog)
        flat.handle_message(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']))
        flat.messages.clear()
        flat.handle_message(RecordMessage('users', {
            'id': 5,
            'name': 'grace',
            'tags': [{'label': 'a', 'weight': 2.0}, {'label': 'b', 'weight': 3.0}],
        }))

        parent = [m for m in flat.by_type('RECORD') if m.stream == 'users'][0].record
        self.assertNotIn('name', parent)
        self.assertEqual(parent['id'], 5)
        self.assertIn('tags_array_status', parent)

        tag_records = [m.record for m in flat.by_type('RECORD') if m.stream == 'tags']
        for child in tag_records:
            self.assertNotIn('weight', child)
            self.assertIn('label', child)
            self.assertIn('id', child)
            self.assertIn('_sdc_source_array_index', child)

    def test_unsupported_inclusion_is_never_synced(self):
        self.catalog['streams'][0]['metadata'].append(
            {'breadcrumb': ('properties', 'name'),
             'metadata': {'inclusion': 'unsupported', 'selected': True}})
        flat = CollectingFlattener(two_array_config(), catalog=self.catalog)
        flat.handle_message(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']))
        flat.messages.clear()
        flat.handle_message(RecordMessage('users', {
            'id': 5, 'name': 'grace', 'tags': []}))
        parent = flat.by_type('RECORD')[0].record
        self.assertNotIn('name', parent)

    def test_without_catalog_every_field_is_retained(self):
        flat = CollectingFlattener(two_array_config())
        flat.handle_message(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']))
        flat.messages.clear()
        flat.handle_message(RecordMessage('users', {
            'id': 5, 'name': 'grace', 'tags': [{'label': 'a', 'weight': 1.0}]}))
        parent = flat.by_type('RECORD')[0].record
        self.assertIn('name', parent)
        child = [m for m in flat.by_type('RECORD') if m.stream == 'tags'][0]
        self.assertIn('weight', child.record)


class TestDefaultWriterAndEdgeCases(unittest.TestCase):
    def test_default_writer_writes_json_lines_to_stdout(self):
        flat = ArrayFlattener(two_array_config())
        schema = SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id'])
        state = StateMessage({'bookmark': 1})
        with mock.patch('sys.stdout', new=io.StringIO()) as fake_stdout:
            flat.handle_message(schema)
            flat.handle_message(state)
            output = fake_stdout.getvalue()

        lines = output.splitlines()
        self.assertEqual(len(lines), 4)
        for line in lines[:3]:
            self.assertEqual(json.loads(line)['type'], 'SCHEMA')
        self.assertEqual(json.loads(lines[3])['type'], 'STATE')

    def test_schema_write_failure_marks_flattener_failed(self):
        calls = {'count': 0}

        def failing_writer(message):
            calls['count'] += 1
            if calls['count'] == 2:
                raise IOError('down')

        flat = ArrayFlattener(two_array_config(), writer=failing_writer)
        with self.assertRaisesRegex(IOError, 'down'):
            flat.handle_message(SchemaMessage(
                stream='users', schema=parent_schema(), key_properties=['id']))
        with self.assertRaisesRegex(ArrayFlattenerSyncError, 'already failed'):
            flat.handle_message(StateMessage({'bookmark': 1}))
        self.assertEqual(calls['count'], 2)

    def test_unrecognized_message_is_rejected(self):
        flat = CollectingFlattener(two_array_config())
        with self.assertRaisesRegex(ArrayFlattenerSyncError, 'unrecognized'):
            flat.handle_message(None)

    def test_consume_lines_skips_blank_lines(self):
        flat = CollectingFlattener(two_array_config())
        schema_line = json.dumps(SchemaMessage(
            stream='users', schema=parent_schema(), key_properties=['id']).asdict())
        record_line = json.dumps(RecordMessage(
            'users', {'id': 1, 'tags': []}).asdict())
        flat.consume_lines(['', schema_line, '   ', record_line, '\n'])
        self.assertTrue(any(m.asdict()['type'] == 'RECORD' for m in flat.messages))
