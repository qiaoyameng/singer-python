import copy
import unittest

from singer import Catalog, metadata
from singer.catalog_migration import (
    CatalogMigrationInputChanged,
    RenameMappingError,
    apply_migration,
    plan_migration,
)


def old_catalog():
    '''The previously edited catalog: user selections on an old schema.'''
    return {
        'streams': [{
            'tap_stream_id': 'orders',
            'stream': 'orders',
            'key_properties': ['legacy_id'],
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {'type': ['null', 'integer']},
                    'legacy_id': {'type': ['null', 'string']},
                    'full_name': {'type': ['null', 'string']},
                    'amount': {'type': ['null', 'string']},
                    'tags': {
                        'type': ['null', 'array'],
                        'items': {'type': ['null', 'string']},
                    },
                    'lines': {
                        'type': ['null', 'array'],
                        'items': {
                            'type': 'object',
                            'properties': {
                                'note': {'type': ['null', 'string']},
                            },
                        },
                    },
                    'untouched': {'type': ['null', 'string']},
                },
            },
            'metadata': [
                {'breadcrumb': [], 'metadata': {'selected': True}},
                {'breadcrumb': ['properties', 'id'],
                 'metadata': {'selected': False, 'inclusion': 'available'}},
                {'breadcrumb': ['properties', 'legacy_id'],
                 'metadata': {'selected': True, 'inclusion': 'automatic'}},
                {'breadcrumb': ['properties', 'full_name'],
                 'metadata': {'selected': True, 'inclusion': 'available',
                              'custom-ui-flag': 'do-not-migrate'}},
                {'breadcrumb': ['properties', 'amount'],
                 'metadata': {'selected': True}},
                {'breadcrumb': ['properties', 'tags'],
                 'metadata': {'selected': True}},
                {'breadcrumb': ['properties', 'lines', 'items',
                                'properties', 'note'],
                 'metadata': {'selected': True}},
            ],
        }],
    }


def new_catalog():
    '''Re-discovery: pk change, rename, type change, nesting change,
    and an array subfield moved to the top level.'''
    return {
        'streams': [{
            'tap_stream_id': 'orders',
            'stream': 'orders',
            'key_properties': ['id'],
            'replication_key': 'updated_at',
            'schema': {
                'type': 'object',
                'properties': {
                    'id': {'type': ['null', 'integer']},
                    'display_name': {'type': ['null', 'string']},
                    'amount': {'type': ['null', 'number']},
                    'tags': {
                        'type': ['null', 'array'],
                        'items': {
                            'type': ['null', 'array'],
                            'items': {'type': ['null', 'string']},
                        },
                    },
                    'note': {'type': ['null', 'string']},
                    'updated_at': {'type': ['null', 'string'],
                                   'format': 'date-time'},
                    'untouched': {'type': ['null', 'string']},
                },
            },
            'metadata': [
                {'breadcrumb': [], 'metadata': {'inclusion': 'available'}},
                {'breadcrumb': ['properties', 'id'],
                 'metadata': {'inclusion': 'automatic'}},
                {'breadcrumb': ['properties', 'display_name'],
                 'metadata': {'inclusion': 'available'}},
                {'breadcrumb': ['properties', 'amount'],
                 'metadata': {'inclusion': 'available'}},
                {'breadcrumb': ['properties', 'note'],
                 'metadata': {'inclusion': 'available'}},
                {'breadcrumb': ['properties', 'updated_at'],
                 'metadata': {'inclusion': 'automatic'}},
                {'breadcrumb': ['properties', 'untouched'],
                 'metadata': {'inclusion': 'available',
                              'fresh-default': 'from-new-discovery'}},
            ],
        }],
    }


RENAMES = {
    ('properties', 'full_name'): ('properties', 'display_name'),
    ('properties', 'lines', 'items', 'properties', 'note'): ('properties', 'note'),
}


def selected_of(catalog_dict, stream_id):
    stream = next(s for s in catalog_dict['streams']
                  if s['tap_stream_id'] == stream_id)
    mdata = metadata.to_map(stream['metadata'])
    return {bc: entry.get('selected') for bc, entry in mdata.items()
            if 'selected' in entry}


class TestMigrationExample(unittest.TestCase):
    '''Array subfield move + one explicit rename + primary key change.'''

    def test_plan_kept_conflicts_removed(self):
        plan = plan_migration(old_catalog(), new_catalog(), renames=RENAMES)

        kept = {(k['stream'], tuple(k['breadcrumb'])): k['metadata']
                for k in plan.kept}
        # Stream-level selection and the explicit rename are kept.
        self.assertEqual(kept[('orders', ())], {'selected': True})
        self.assertEqual(kept[('orders', ('properties', 'display_name'))],
                         {'selected': True})
        # The array subfield moved to top level via the explicit rename.
        self.assertEqual(kept[('orders', ('properties', 'note'))],
                         {'selected': True})
        # Only selection metadata migrates, never other keys.
        for entry in plan.kept:
            self.assertEqual(set(entry['metadata']), {'selected'})

        # Same-name fields whose type or array nesting changed conflict.
        conflicts = {(c['stream'], tuple(c['breadcrumb'])): c['reason']
                     for c in plan.conflicts}
        self.assertEqual(conflicts[('orders', ('properties', 'amount'))],
                         'type-changed')
        self.assertEqual(conflicts[('orders', ('properties', 'tags'))],
                         'array-nesting-changed')

        # The old primary key is gone from the new schema: diagnostic.
        removed = {(r['stream'], tuple(r['breadcrumb'])): r['reason']
                   for r in plan.removed}
        self.assertEqual(removed[('orders', ('properties', 'legacy_id'))],
                         'field-removed')

        # Primary key change: old selected=False cannot disable the new
        # required key property.
        self.assertEqual(
            [(o['stream'], tuple(o['breadcrumb'])) for o in plan.overrides],
            [('orders', ('properties', 'id'))])

        renamed = {tuple(r['old_breadcrumb']): tuple(r['new_breadcrumb'])
                   for r in plan.renamed}
        self.assertEqual(renamed[('properties', 'full_name')],
                         ('properties', 'display_name'))

    def test_apply_produces_migrated_catalog(self):
        old, new = old_catalog(), new_catalog()
        plan = plan_migration(old, new, renames=RENAMES)
        migrated = apply_migration(plan, old, new, renames=RENAMES)
        result = migrated.to_dict()

        selected = selected_of(result, 'orders')
        self.assertEqual(selected[()], True)
        self.assertEqual(selected[('properties', 'display_name')], True)
        self.assertEqual(selected[('properties', 'note')], True)
        # Required field stays enabled: no selected=False on id.
        self.assertNotEqual(selected.get(('properties', 'id')), False)
        # Conflicting fields keep new discovery defaults (no selection).
        self.assertNotIn(('properties', 'amount'), selected)
        self.assertNotIn(('properties', 'tags'), selected)

        # The new schema wins; nothing from the old schema leaks back.
        stream = result['streams'][0]
        self.assertEqual(stream['schema'], new['streams'][0]['schema'])
        self.assertEqual(stream['key_properties'], ['id'])
        self.assertEqual(stream['replication_key'], 'updated_at')

        # Unedited fields keep the new discovery defaults untouched.
        mdata = metadata.to_map(stream['metadata'])
        self.assertEqual(
            mdata[('properties', 'untouched')],
            {'inclusion': 'available', 'fresh-default': 'from-new-discovery'})
        # Non-selection metadata from the old catalog is not migrated.
        self.assertNotIn('custom-ui-flag',
                         mdata[('properties', 'display_name')])

    def test_inputs_are_not_mutated(self):
        old, new = old_catalog(), new_catalog()
        old_snapshot = copy.deepcopy(old)
        new_snapshot = copy.deepcopy(new)
        plan = plan_migration(old, new, renames=RENAMES)
        apply_migration(plan, old, new, renames=RENAMES)
        self.assertEqual(old, old_snapshot)
        self.assertEqual(new, new_snapshot)


class TestRenameValidation(unittest.TestCase):
    def test_rename_must_be_one_to_one(self):
        with self.assertRaises(RenameMappingError):
            plan_migration(old_catalog(), new_catalog(), renames={
                ('properties', 'full_name'): ('properties', 'display_name'),
                ('properties', 'amount'): ('properties', 'display_name'),
            })

    def test_rename_must_be_acyclic(self):
        with self.assertRaises(RenameMappingError):
            plan_migration(old_catalog(), new_catalog(), renames={
                ('properties', 'full_name'): ('properties', 'amount'),
                ('properties', 'amount'): ('properties', 'full_name'),
            })

    def test_rename_target_must_not_collide_with_identity_match(self):
        with self.assertRaises(RenameMappingError):
            plan_migration(old_catalog(), new_catalog(), renames={
                ('properties', 'full_name'): ('properties', 'amount'),
            })

    def test_rename_to_self_rejected(self):
        with self.assertRaises(RenameMappingError):
            plan_migration(old_catalog(), new_catalog(), renames={
                ('properties', 'amount'): ('properties', 'amount'),
            })

    def test_missing_rename_target_is_a_diagnostic(self):
        plan = plan_migration(old_catalog(), new_catalog(), renames={
            ('properties', 'full_name'): ('properties', 'no_such_field'),
        })
        self.assertEqual(plan.removed[0]['reason'], 'rename-target-missing')
        self.assertEqual(plan.removed[0]['old_breadcrumb'],
                         ['properties', 'full_name'])


class TestPlanBinding(unittest.TestCase):
    def test_apply_rejects_changed_new_catalog(self):
        old, new = old_catalog(), new_catalog()
        plan = plan_migration(old, new, renames=RENAMES)
        tampered = copy.deepcopy(new)
        tampered['streams'][0]['schema']['properties']['extra'] = {
            'type': 'string'}
        with self.assertRaises(CatalogMigrationInputChanged):
            apply_migration(plan, old, tampered, renames=RENAMES)

    def test_apply_rejects_changed_old_catalog(self):
        old, new = old_catalog(), new_catalog()
        plan = plan_migration(old, new, renames=RENAMES)
        tampered = copy.deepcopy(old)
        tampered['streams'][0]['metadata'][0]['metadata']['selected'] = False
        with self.assertRaises(CatalogMigrationInputChanged):
            apply_migration(plan, tampered, new, renames=RENAMES)

    def test_apply_rejects_changed_renames(self):
        old, new = old_catalog(), new_catalog()
        plan = plan_migration(old, new, renames=RENAMES)
        with self.assertRaises(CatalogMigrationInputChanged):
            apply_migration(plan, old, new, renames={
                ('properties', 'full_name'): ('properties', 'display_name'),
            })

    def test_plan_digests_are_stable(self):
        plan_a = plan_migration(old_catalog(), new_catalog(), renames=RENAMES)
        plan_b = plan_migration(old_catalog(), new_catalog(), renames=RENAMES)
        self.assertEqual(plan_a.to_dict(), plan_b.to_dict())


class TestEdgeCases(unittest.TestCase):
    def test_removed_stream_selections_become_diagnostics(self):
        old = old_catalog()
        new = {'streams': []}
        plan = plan_migration(old, new)
        self.assertTrue(plan.removed)
        self.assertTrue(all(r['reason'] == 'stream-removed'
                            for r in plan.removed))
        self.assertEqual(plan.kept, [])

    def test_accepts_catalog_objects(self):
        old = Catalog.from_dict(copy.deepcopy(old_catalog()))
        new = Catalog.from_dict(copy.deepcopy(new_catalog()))
        plan = plan_migration(old, new, renames=RENAMES)
        migrated = apply_migration(plan, old, new, renames=RENAMES)
        self.assertIsInstance(migrated, Catalog)
        selected = selected_of(migrated.to_dict(), 'orders')
        self.assertEqual(selected[('properties', 'display_name')], True)

    def test_no_guessing_without_explicit_rename(self):
        # full_name -> display_name is obvious to a human, but without
        # an explicit mapping the selection must not be carried over.
        plan = plan_migration(old_catalog(), new_catalog())
        kept_breadcrumbs = {tuple(k['breadcrumb']) for k in plan.kept}
        self.assertNotIn(('properties', 'display_name'), kept_breadcrumbs)
        removed = {tuple(r['breadcrumb']) for r in plan.removed}
        self.assertIn(('properties', 'full_name'), removed)


if __name__ == '__main__':
    unittest.main()
