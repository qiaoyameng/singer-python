'''Plan-and-apply migration of catalog selection metadata across re-discovery.

When a tap is re-discovered, the new catalog reflects the current source
schema, but any fields a user manually selected in the old catalog are
lost. This module migrates only the selection-related metadata (the
``selected`` keys) from an old catalog onto a new discovery catalog:

1. ``plan_migration(old_catalog, new_catalog, renames=None)`` inspects
   both catalogs and returns a :class:`MigrationPlan` describing exactly
   which selections would be kept, which conflict with the new schema,
   and which refer to removed fields.
2. ``apply_migration(plan, old_catalog, new_catalog, renames=None)``
   re-validates that both inputs are unchanged since planning (via
   content digests bound into the plan) and returns a new ``Catalog``
   with the planned selections applied.

Matching is done on structural metadata breadcrumbs (tuples such as
``('properties', 'lines', 'items', 'properties', 'note')``), never on
dot-joined path strings, so array nesting is preserved exactly. A field
whose type or array nesting level changed between versions is a
conflict and its selection is not carried over. Renames are only
applied when given explicitly; nothing is guessed.

Only selection metadata is migrated. The new catalog's schema, key
properties, replication metadata and all non-selection metadata are
kept as discovery produced them, and fields the user never edited keep
the new discovery defaults. Required fields (new key properties, the
replication key, and anything the new catalog marks
``inclusion: automatic``) always win: an old ``selected: False`` can
never disable them.

Neither input catalog is mutated by planning or applying.
'''

import copy
import hashlib
import json

from . import metadata as metadata_module
from .catalog import Catalog
from .exceptions import SingerError

_SELECTION_KEYS = ('selected',)


class CatalogMigrationError(SingerError):
    """Base class for catalog selection migration errors."""


class RenameMappingError(CatalogMigrationError):
    """The explicit rename mapping is not one-to-one or contains a cycle."""


class CatalogMigrationInputChanged(CatalogMigrationError):
    """An input changed between planning and applying the migration."""


def _canonical_json(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), default=str)


def _digest(obj):
    return hashlib.sha256(_canonical_json(obj).encode('utf-8')).hexdigest()


def _catalog_dict(catalog):
    if isinstance(catalog, Catalog):
        return catalog.to_dict()
    return catalog


def _normalize_breadcrumb(breadcrumb):
    if isinstance(breadcrumb, (list, tuple)):
        return tuple(str(part) for part in breadcrumb)
    raise RenameMappingError(
        f"Rename mapping breadcrumbs must be lists or tuples, got: {breadcrumb!r}")


def _normalize_renames(renames):
    '''Normalize a rename mapping to {tuple: tuple} and validate it.

    The mapping must be one-to-one (no two sources may map to the same
    target) and acyclic (a -> b and b -> a, or longer loops, are
    rejected). Renames are explicit only; nothing is inferred.
    '''
    normalized = {}
    if not renames:
        return normalized
    for raw_source, raw_target in renames.items():
        source = _normalize_breadcrumb(raw_source)
        target = _normalize_breadcrumb(raw_target)
        if source in normalized:
            raise RenameMappingError(
                f"Duplicate rename source: {source!r}")
        if source == target:
            raise RenameMappingError(
                f"Rename maps a field to itself: {source!r}")
        normalized[source] = target

    targets = list(normalized.values())
    if len(set(targets)) != len(targets):
        raise RenameMappingError(
            f"Rename mapping must be one-to-one; duplicate targets in: {targets!r}")

    # Every source has out-degree 1, so following the chain from any
    # source detects cycles by revisiting a node in the current walk.
    for start in normalized:
        seen = set()
        node = start
        while node in normalized:
            if node in seen:
                raise RenameMappingError(
                    f"Rename mapping contains a cycle involving: {node!r}")
            seen.add(node)
            node = normalized[node]
    return normalized


def _walk_schema(node, breadcrumb=()):
    '''Yield (breadcrumb, subschema) for every property and items node.

    Breadcrumbs are structural tuples alternating 'properties'/<name>
    and 'items', matching singer metadata breadcrumbs exactly.
    '''
    if not isinstance(node, dict):
        return
    for name, subschema in (node.get('properties') or {}).items():
        child = breadcrumb + ('properties', name)
        yield child, subschema
        yield from _walk_schema(subschema, child)
    items = node.get('items')
    if isinstance(items, dict):
        child = breadcrumb + ('items',)
        yield child, items
        yield from _walk_schema(items, child)


def _shape(node):
    '''Structural signature of a schema node: types + array nesting.

    Used to detect type changes and array-nesting-level changes for a
    same-named field. Object children are compared individually via
    their own breadcrumbs, so they are not part of the parent shape.
    '''
    if not isinstance(node, dict):
        return ((), None)
    raw_type = node.get('type')
    if isinstance(raw_type, list):
        types = tuple(sorted(t for t in raw_type if t != 'null'))
    elif isinstance(raw_type, str):
        types = (raw_type,)
    elif 'properties' in node:
        types = ('object',)
    elif 'items' in node:
        types = ('array',)
    else:
        types = ()
    items = node.get('items')
    items_shape = _shape(items) if isinstance(items, dict) else None
    return (types, items_shape)


def _stream_id(entry_dict):
    return entry_dict.get('tap_stream_id') or entry_dict.get('stream')


def _required_breadcrumbs(stream_dict):
    '''Breadcrumbs the new discovery requires to be synced.

    A field is required if it is a new key property, the replication
    key, or its new metadata says inclusion: automatic. These take
    priority over any migrated selection.
    '''
    required = set()
    mdata = metadata_module.to_map(stream_dict.get('metadata') or [])
    for breadcrumb, entry in mdata.items():
        if entry.get('inclusion') == 'automatic':
            required.add(breadcrumb)
    for name in stream_dict.get('key_properties') or []:
        required.add(('properties', name))
    replication_key = stream_dict.get('replication_key')
    if replication_key:
        required.add(('properties', replication_key))
    return required


class MigrationPlan():
    '''The result of planning a selection migration.

    Attributes:
        kept: selections that will be carried onto the new catalog.
            Each entry: {'stream', 'breadcrumb', 'metadata'} where
            'metadata' holds only selection keys (e.g. {'selected': True}).
        conflicts: same-named fields whose type or array nesting level
            changed; their selections are not migrated.
        removed: selections pointing at fields (or streams) that no
            longer exist in the new discovery. Kept as diagnostics.
        renamed: explicit renames that were applied, each with
            'old_breadcrumb' and 'new_breadcrumb'.
        overrides: required fields (new key properties, replication
            key, inclusion=automatic) where an old selected=False was
            dropped because it cannot disable a required field.
        old_digest/new_digest/renames_digest: content digests of the
            inputs at plan time; apply_migration refuses to run if the
            inputs it is given no longer match.
    '''

    def __init__(self, kept=None, conflicts=None, removed=None, renamed=None,
                 overrides=None, old_digest=None, new_digest=None,
                 renames_digest=None):
        self.kept = kept or []
        self.conflicts = conflicts or []
        self.removed = removed or []
        self.renamed = renamed or []
        self.overrides = overrides or []
        self.old_digest = old_digest
        self.new_digest = new_digest
        self.renames_digest = renames_digest

    def to_dict(self):
        return {
            'kept': self.kept,
            'conflicts': self.conflicts,
            'removed': self.removed,
            'renamed': self.renamed,
            'overrides': self.overrides,
            'old_digest': self.old_digest,
            'new_digest': self.new_digest,
            'renames_digest': self.renames_digest,
        }

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return (f"MigrationPlan(kept={len(self.kept)}, "
                f"conflicts={len(self.conflicts)}, "
                f"removed={len(self.removed)}, "
                f"renamed={len(self.renamed)}, "
                f"overrides={len(self.overrides)})")


def _selection_metadata(entry):
    return {k: v for k, v in entry.items() if k in _SELECTION_KEYS}


def plan_migration(old_catalog, new_catalog, renames=None):
    '''Plan migrating selection metadata from an old catalog to a new one.

    Args:
        old_catalog: Catalog or dict; the previously edited catalog.
        new_catalog: Catalog or dict; the fresh re-discovery catalog.
        renames: optional explicit mapping of old breadcrumbs to new
            breadcrumbs (lists or tuples). Must be one-to-one and
            acyclic. No renaming is ever inferred automatically.

    Returns a MigrationPlan. Neither input is mutated.
    '''
    old_dict = _catalog_dict(old_catalog)
    new_dict = _catalog_dict(new_catalog)
    rename_map = _normalize_renames(renames)

    plan = MigrationPlan(
        old_digest=_digest(old_dict),
        new_digest=_digest(new_dict),
        renames_digest=_digest(sorted(
            [list(source), list(target)] for source, target in rename_map.items())),
    )

    new_streams = {}
    for stream in new_dict.get('streams') or []:
        new_streams[_stream_id(stream)] = stream

    for old_stream in old_dict.get('streams') or []:
        stream_id = _stream_id(old_stream)
        old_mdata = metadata_module.to_map(old_stream.get('metadata') or [])
        old_selections = {
            breadcrumb: _selection_metadata(entry)
            for breadcrumb, entry in old_mdata.items()
            if _selection_metadata(entry)
        }
        if not old_selections:
            continue

        new_stream = new_streams.get(stream_id)
        if new_stream is None:
            for breadcrumb in sorted(old_selections):
                plan.removed.append({
                    'stream': stream_id,
                    'breadcrumb': list(breadcrumb),
                    'reason': 'stream-removed',
                })
            continue

        old_fields = dict(_walk_schema(old_stream.get('schema') or {}))
        new_fields = dict(_walk_schema(new_stream.get('schema') or {}))
        required = _required_breadcrumbs(new_stream)

        # Renames compose with implicit identity matches, so a rename
        # target must not collide with a field that keeps its name.
        claimed_targets = {}
        for breadcrumb in old_fields:
            target = rename_map.get(breadcrumb, breadcrumb)
            if target in claimed_targets and claimed_targets[target] != breadcrumb:
                raise RenameMappingError(
                    f"Rename target {target!r} in stream {stream_id!r} collides "
                    f"with existing field {claimed_targets[target]!r}")
            claimed_targets[target] = breadcrumb

        for breadcrumb in sorted(old_selections):
            selection = old_selections[breadcrumb]
            target = rename_map.get(breadcrumb, breadcrumb)
            renamed = target != breadcrumb

            if breadcrumb == ():
                # Stream-level selection always carries over.
                plan.kept.append({
                    'stream': stream_id,
                    'breadcrumb': [],
                    'metadata': selection,
                })
                continue

            if breadcrumb not in old_fields:
                plan.removed.append({
                    'stream': stream_id,
                    'breadcrumb': list(breadcrumb),
                    'reason': 'breadcrumb-not-in-old-schema',
                })
                continue

            if target not in new_fields:
                plan.removed.append({
                    'stream': stream_id,
                    'breadcrumb': list(target),
                    'old_breadcrumb': list(breadcrumb),
                    'reason': 'rename-target-missing' if renamed else 'field-removed',
                })
                continue

            old_shape = _shape(old_fields[breadcrumb])
            new_shape = _shape(new_fields[target])
            if old_shape != new_shape:
                plan.conflicts.append({
                    'stream': stream_id,
                    'breadcrumb': list(target),
                    'old_breadcrumb': list(breadcrumb),
                    'reason': 'type-changed' if old_shape[0] != new_shape[0]
                              else 'array-nesting-changed',
                    'old_shape': _canonical_json(old_shape),
                    'new_shape': _canonical_json(new_shape),
                })
                continue

            if renamed:
                plan.renamed.append({
                    'stream': stream_id,
                    'old_breadcrumb': list(breadcrumb),
                    'new_breadcrumb': list(target),
                })

            if target in required and selection.get('selected') is False:
                plan.overrides.append({
                    'stream': stream_id,
                    'breadcrumb': list(target),
                    'reason': 'required-field-cannot-be-deselected',
                })
                continue

            plan.kept.append({
                'stream': stream_id,
                'breadcrumb': list(target),
                'metadata': selection,
            })

    return plan


def apply_migration(plan, old_catalog, new_catalog, renames=None):
    '''Apply a MigrationPlan, returning a new Catalog.

    Re-computes the input digests and refuses to run if old_catalog,
    new_catalog or renames changed since the plan was created. The
    result is built from the new catalog: only the planned selection
    metadata is written onto it, so schemas and all other metadata stay
    exactly as the new discovery produced them. Neither input is
    mutated.
    '''
    old_dict = _catalog_dict(old_catalog)
    new_dict = _catalog_dict(new_catalog)
    rename_map = _normalize_renames(renames)

    expected = {
        'old_catalog': (plan.old_digest, _digest(old_dict)),
        'new_catalog': (plan.new_digest, _digest(new_dict)),
        'renames': (plan.renames_digest, _digest(sorted(
            [list(source), list(target)] for source, target in rename_map.items()))),
    }
    for name, (planned, actual) in expected.items():
        if planned != actual:
            raise CatalogMigrationInputChanged(
                f"{name} changed after the migration plan was created; "
                f"re-run plan_migration. planned={planned} actual={actual}")

    result = copy.deepcopy(new_dict)
    streams = {_stream_id(stream): stream for stream in result.get('streams') or []}

    for kept in plan.kept:
        stream = streams.get(kept['stream'])
        if stream is None:
            raise CatalogMigrationInputChanged(
                f"Stream {kept['stream']!r} from the plan is missing in new_catalog")
        mdata = metadata_module.to_map(stream.get('metadata') or [])
        breadcrumb = tuple(kept['breadcrumb'])
        for key, value in kept['metadata'].items():
            metadata_module.write(mdata, breadcrumb, key, value)
        stream['metadata'] = metadata_module.to_list(mdata)

    return Catalog.from_dict(result)
