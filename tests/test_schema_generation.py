import json
import unittest

from singer.schema_generation import (
    DEFAULT_STRATEGY,
    DuplicateShardError,
    IncompleteObservationError,
    STRATEGY_COERCE_NUMERIC_STRINGS,
    STRATEGY_KEEP_STRINGS,
    StrategyMismatchError,
    finalize,
    generate_schema,
    merge,
    observe,
)

class TestSchemaGeneration(unittest.TestCase):
    def test_simple_schema(self):
        records = [{'a': 1, 'b': 'two', 'c': True, 'dt': '2000-01-01T00:11:22Z'}]
        expected_schema = {
            'type': ['null', 'object'],
            'properties': {
                'a': {'type': ['null', 'integer']},
                'b': {'type': ['null', 'string']},
                'c': {'type': ['null', 'boolean']},
                'dt': {'type': ['null', 'string']}
            }
        }
        self.assertEqual(expected_schema, generate_schema(records))


class TestObserveMergeFinalize(unittest.TestCase):

    records = [
        {'id': 1, 'name': 'ann', 'tags': ['x', 'y'],
         'meta': {'score': 1.5, 'flag': True}},
        {'id': 2, 'name': None, 'tags': [],
         'meta': {'score': 2, 'flag': False}, 'extra': 'e'},
        {'id': 3, 'name': 'bob', 'tags': ['z'],
         'meta': {'score': 3.5}},
    ]

    def test_observe_counts(self):
        state = observe(self.records, shard_id='s1')
        root = state['root']
        self.assertTrue(state['complete'])
        self.assertEqual(3, root['samples'])
        self.assertEqual({'object': 3}, root['types'])

        meta = root['fields']['meta']
        self.assertEqual(3, meta['count'])
        # 'flag' is missing from one record: occurrence count differs from
        # the object scope's sample count
        self.assertEqual(2, meta['node']['fields']['flag']['count'])
        self.assertEqual(3, meta['node']['samples'])
        # bool must not count as integer
        self.assertEqual({'boolean': 2},
                         meta['node']['fields']['flag']['node']['types'])
        self.assertEqual({'integer': 1, 'number': 2},
                         meta['node']['fields']['score']['node']['types'])

    def test_missing_field_vs_explicit_null(self):
        state = observe(self.records)
        name = state['root']['fields']['name']['node']
        # present in all three records, explicitly null once
        self.assertEqual(3, name['samples'])
        self.assertEqual(1, name['nulls'])
        self.assertEqual({'string': 2}, name['types'])
        # 'extra' appears only once and is never null
        extra = state['root']['fields']['extra']
        self.assertEqual(1, extra['count'])
        self.assertEqual(0, extra['node']['nulls'])
        self.assertEqual(3, state['root']['fields']['id']['count'])

    def test_empty_array_is_not_null_only_items(self):
        state = observe([{'a': []}, {'a': []}])
        items = state['root']['fields']['a']['node']['items']
        self.assertIsNone(items)
        schema = finalize(state)
        array_schema = schema['properties']['a']
        self.assertEqual(['null', 'array'], array_schema['type'])
        # no items constraint was observed, so none is declared
        self.assertNotIn('items', array_schema)

    def test_numeric_string_strategies(self):
        coerced = observe([{'a': '42'}], strategy=STRATEGY_COERCE_NUMERIC_STRINGS)
        self.assertEqual({'integer': 1},
                         coerced['root']['fields']['a']['node']['types'])
        kept = observe([{'a': '42'}], strategy=STRATEGY_KEEP_STRINGS)
        self.assertEqual({'string': 1},
                         kept['root']['fields']['a']['node']['types'])
        self.assertEqual(STRATEGY_COERCE_NUMERIC_STRINGS, DEFAULT_STRATEGY)

    def test_merge_rejects_strategy_mismatch(self):
        left = observe([{'a': '1'}], shard_id='l',
                       strategy=STRATEGY_COERCE_NUMERIC_STRINGS)
        right = observe([{'a': '2'}], shard_id='r',
                        strategy=STRATEGY_KEEP_STRINGS)
        with self.assertRaises(StrategyMismatchError):
            merge(left, right)

    def test_merge_rejects_duplicate_shard(self):
        shard = observe([{'a': 1}], shard_id='s1')
        other = observe([{'a': 2}], shard_id='s2')
        with self.assertRaises(DuplicateShardError):
            merge(shard, shard)
        merged = merge(shard, other)
        with self.assertRaises(DuplicateShardError):
            merge(merged, observe([{'a': 3}], shard_id='s1'))

    def test_merge_order_and_split_invariance(self):
        split_a = [observe(self.records[:1], shard_id='a1'),
                   observe(self.records[1:], shard_id='a2')]
        split_b = [observe(self.records[:2], shard_id='b1'),
                   observe(self.records[2:], shard_id='b2')]
        merged_a = merge(*split_a)
        merged_b = merge(*reversed(split_b))
        merged_all = merge(observe(self.records, shard_id='all'))

        self.assertEqual(merged_a['root'], merged_b['root'])
        self.assertEqual(merged_a['root'], merged_all['root'])
        self.assertEqual(finalize(merged_a), finalize(merged_b))
        self.assertEqual(finalize(merged_a), finalize(merged_all))

    def test_state_is_json_serializable(self):
        state = observe(self.records, shard_id='s1')
        restored = json.loads(json.dumps(state))
        self.assertEqual(state, restored)
        merged = merge(restored, observe(self.records, shard_id='s2'))
        self.assertEqual(6, merged['root']['samples'])

    def test_finalize_field_order_is_deterministic(self):
        first = observe([{'b': 1, 'a': 1}], shard_id='s1')
        second = observe([{'c': 1, 'a': 2}], shard_id='s2')
        schema = finalize(merge(second, first))
        self.assertEqual(['a', 'b', 'c'], list(schema['properties'].keys()))

    def test_depth_limit_marks_incomplete(self):
        state = observe([{'a': {'b': {'c': 1}}}], max_depth=1)
        self.assertFalse(state['complete'])
        self.assertTrue(state['root']['fields']['a']['node']['truncated'])
        with self.assertRaises(IncompleteObservationError):
            finalize(state)
        schema = finalize(state, allow_incomplete=True)
        self.assertEqual(['null', 'object'],
                         schema['properties']['a']['type'])

    def test_field_limit_marks_incomplete(self):
        state = observe([{'a': 1, 'b': 2, 'c': 3}], max_fields=2)
        self.assertFalse(state['complete'])
        self.assertTrue(state['root']['truncated'])
        self.assertEqual(2, len(state['root']['fields']))
        with self.assertRaises(IncompleteObservationError):
            finalize(state)

    def test_finalize_conservative_schema(self):
        schema = finalize(merge(observe(self.records[:2], shard_id='s1'),
                                observe(self.records[2:], shard_id='s2')))
        self.assertEqual(['null', 'object'], schema['type'])
        self.assertNotIn('required', schema)
        # score saw both integer and number samples
        score = schema['properties']['meta']['properties']['score']
        self.assertEqual(
            {'anyOf': [{'type': ['null', 'string'], 'format': 'singer.decimal'},
                       {'type': ['null', 'integer']}]},
            score)

    def test_mix_n_match_records_schema(self):
        records = [
            {'a': 1, 'b': 'b'},
            {'a': 'two', 'c': 7, 'd': [1, 'two']},
            {'a': True, 'c': 7.7, 'd': {'one': 1, 'two': 'two'}}
        ]
        expected_schema = {
            'type': ['null', 'object'],
            'properties': {'a': {'anyOf': [{'type': ['null', 'integer']},
                                           {'type': ['null', 'boolean']},
                                           {'type': ['null', 'string']}]},
                           'b': {'type': ['null', 'string']},
                           'c': {'anyOf': [{'type': ['null', 'string'], 'format': 'singer.decimal'},
                                           {'type': ['null', 'integer']}]},
                'd': {'anyOf': [{'type': ['null', 'array'],
                                 'items': {'anyOf': [{'type': ['null', 'integer']},
                                                     {'type': ['null', 'string']}]}},
                                {'type': ['null', 'object'],
                                 'properties': {'one': {'type': ['null', 'integer']},
                                                'two': {'type': ['null', 'string']}}}]}}
        }
        actual_schema = generate_schema(records)
        self.assertEqual(expected_schema, actual_schema)

    def test_nested_structue_schema(self):
        records = [{'a': {'b': {'c': [{'d': 7}]}, 'e': [[1, 2, 3]]}}]
        expected_schema = {
            'type': ['null', 'object'],
            'properties': {
                'a': {
                    'type': ['null', 'object'],
                    'properties': {
                        'b': {
                            'type': ['null', 'object'],
                            'properties': {
                                'c': {
                                    'type': ['null', 'array'],
                                    'items': {
                                        'type': ['null', 'object'],
                                        'properties': {'d': {'type': ['null', 'integer']}}
                                    }
                                }
                            }
                        },
                        'e': {
                            'type': ['null', 'array'],
                            'items': {
                                'type': ['null', 'array'],
                                'items': {'type': ['null', 'integer']}}
                        }
                    }
                }
            }
        }
        self.assertEqual(expected_schema, generate_schema(records))
