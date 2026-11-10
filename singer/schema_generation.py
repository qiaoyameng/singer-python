"""Schema generation from observed records.

This module provides a three-phase interface for building a Singer schema
from records that are collected in separate shards:

1. ``observe``   - accumulate a JSON-serializable observation state for the
                   records seen by one shard.
2. ``merge``     - combine observation states from multiple shards. The
                   result is independent of merge order.
3. ``finalize``  - derive a conservative Singer schema from a merged state.

The legacy ``generate_schema`` entry point is kept unchanged for backwards
compatibility.
"""

OBSERVATION_FORMAT = "singer.schema_observation/1"

# Numeric-string handling strategies. Every shard that is merged together
# must use the same strategy.
STRATEGY_COERCE_NUMERIC_STRINGS = "coerce_numeric_strings"
STRATEGY_KEEP_STRINGS = "keep_strings"
VALID_STRATEGIES = (STRATEGY_COERCE_NUMERIC_STRINGS, STRATEGY_KEEP_STRINGS)
DEFAULT_STRATEGY = STRATEGY_COERCE_NUMERIC_STRINGS

# Limits guarding the observation state. Exceeding a limit marks the state
# incomplete instead of silently dropping data. ``None`` means unlimited.
DEFAULT_MAX_FIELDS = 1000
DEFAULT_MAX_DEPTH = 32

# Order in which observed types appear in anyOf lists.
_TYPE_ORDER = ['array', 'object', 'number', 'integer', 'boolean', 'string']


class SchemaObservationError(Exception):
    """Base class for observation-state errors."""


class StrategyMismatchError(SchemaObservationError):
    """Raised when merging states observed with different strategies."""


class DuplicateShardError(SchemaObservationError):
    """Raised when the same shard id is merged more than once."""


class IncompleteObservationError(SchemaObservationError):
    """Raised when finalizing a state that hit observation limits."""


def _new_node():
    return {
        "samples": 0,      # values observed at this scope
        "nulls": 0,        # explicit null values
        "types": {},       # real value type -> count
        "fields": {},      # object scopes: name -> {"count", "node"}
        "items": None,     # array scopes: element node, None if unobserved
        "truncated": False,  # limits were hit at or below this scope
    }


def _mark_truncated(node, state):
    node["truncated"] = True
    state["complete"] = False


def _observe_string(node, value, strategy):
    if strategy == STRATEGY_COERCE_NUMERIC_STRINGS:
        try:
            int(value)
            node["types"]["integer"] = node["types"].get("integer", 0) + 1
            return
        except (ValueError, TypeError):
            pass
        try:
            float(value)
            node["types"]["number"] = node["types"].get("number", 0) + 1
            return
        except (ValueError, TypeError):
            pass
    node["types"]["string"] = node["types"].get("string", 0) + 1


# pylint: disable=too-many-branches
def _observe_value(node, value, depth, max_fields, max_depth, strategy, state):
    node["samples"] += 1

    if value is None:
        node["nulls"] += 1
    elif isinstance(value, bool):
        # bool is checked before int: booleans never count as integers
        node["types"]["boolean"] = node["types"].get("boolean", 0) + 1
    elif isinstance(value, int):
        node["types"]["integer"] = node["types"].get("integer", 0) + 1
    elif isinstance(value, float):
        node["types"]["number"] = node["types"].get("number", 0) + 1
    elif isinstance(value, str):
        _observe_string(node, value, strategy)
    elif isinstance(value, dict):
        node["types"]["object"] = node["types"].get("object", 0) + 1
        if max_depth is not None and depth + 1 > max_depth:
            _mark_truncated(node, state)
            return
        for key, sub_value in value.items():
            entry = node["fields"].get(key)
            if entry is None:
                if max_fields is not None and len(node["fields"]) >= max_fields:
                    _mark_truncated(node, state)
                    continue
                entry = {"count": 0, "node": _new_node()}
                node["fields"][key] = entry
            entry["count"] += 1
            _observe_value(entry["node"], sub_value, depth + 1,
                           max_fields, max_depth, strategy, state)
    elif isinstance(value, list):
        node["types"]["array"] = node["types"].get("array", 0) + 1
        if not value:
            # An empty array means no elements have been observed yet; it
            # says nothing about the element type.
            return
        if max_depth is not None and depth + 1 > max_depth:
            _mark_truncated(node, state)
            return
        if node["items"] is None:
            node["items"] = _new_node()
        for item in value:
            _observe_value(node["items"], item, depth + 1,
                           max_fields, max_depth, strategy, state)
    else:
        node["types"]["string"] = node["types"].get("string", 0) + 1


def observe(records, shard_id=None, strategy=DEFAULT_STRATEGY,
            max_fields=DEFAULT_MAX_FIELDS, max_depth=DEFAULT_MAX_DEPTH):
    """Observe an iterable of records and return a JSON-serializable state.

    ``shard_id`` identifies the collecting shard so that ``merge`` can
    reject merging the same shard twice. ``strategy`` controls whether
    numeric strings are coerced to integer/number observations; all shards
    that are merged together must use the same strategy. ``max_fields``
    limits the number of distinct fields per object scope and ``max_depth``
    limits nesting depth; exceeding a limit yields an incomplete state
    rather than silently dropping fields.
    """
    if strategy not in VALID_STRATEGIES:
        raise ValueError("Unknown strategy {!r}, expected one of {}".format(
            strategy, VALID_STRATEGIES))
    state = {
        "format": OBSERVATION_FORMAT,
        "strategy": strategy,
        "complete": True,
        "shards": [] if shard_id is None else [shard_id],
        "limits": {"max_fields": max_fields, "max_depth": max_depth},
        "root": _new_node(),
    }
    for record in records:
        _observe_value(state["root"], record, 0,
                       max_fields, max_depth, strategy, state)
    return state


def _check_state(state):
    if not isinstance(state, dict) or state.get("format") != OBSERVATION_FORMAT:
        raise SchemaObservationError(
            "Not a {} observation state".format(OBSERVATION_FORMAT))


def _merge_nodes(nodes):
    out = _new_node()
    out["samples"] = sum(node["samples"] for node in nodes)
    out["nulls"] = sum(node["nulls"] for node in nodes)
    out["truncated"] = any(node["truncated"] for node in nodes)
    for node in nodes:
        for type_name, count in node["types"].items():
            out["types"][type_name] = out["types"].get(type_name, 0) + count
    field_names = set()
    for node in nodes:
        field_names.update(node["fields"])
    for name in sorted(field_names):
        field_nodes = [node["fields"][name]["node"]
                       for node in nodes if name in node["fields"]]
        out["fields"][name] = {
            "count": sum(node["fields"][name]["count"]
                         for node in nodes if name in node["fields"]),
            "node": _merge_nodes(field_nodes),
        }
    item_nodes = [node["items"] for node in nodes if node["items"] is not None]
    out["items"] = _merge_nodes(item_nodes) if item_nodes else None
    return out


def merge(*states):
    """Merge observation states from multiple shards into one state.

    Accepts states as varargs or as a single list. All states must use the
    same numeric-string strategy, and merging the same shard id twice is
    rejected with ``DuplicateShardError``. The merged state is independent
    of the order in which states are merged.
    """
    if len(states) == 1 and isinstance(states[0], (list, tuple)):
        states = tuple(states[0])
    if not states:
        raise ValueError("merge requires at least one observation state")
    for state in states:
        _check_state(state)

    strategy = states[0]["strategy"]
    for state in states[1:]:
        if state["strategy"] != strategy:
            raise StrategyMismatchError(
                "Cannot merge states with strategies {!r} and {!r}".format(
                    strategy, state["strategy"]))

    seen_shards = set()
    merged_shards = []
    for state in states:
        for shard_id in state.get("shards", []):
            if shard_id in seen_shards:
                raise DuplicateShardError(
                    "Shard {!r} has already been merged".format(shard_id))
            seen_shards.add(shard_id)
            merged_shards.append(shard_id)

    return {
        "format": OBSERVATION_FORMAT,
        "strategy": strategy,
        "complete": all(state["complete"] for state in states),
        "shards": merged_shards,
        "limits": {
            "max_fields": min((state["limits"]["max_fields"]
                               for state in states
                               if state["limits"]["max_fields"] is not None),
                              default=None),
            "max_depth": min((state["limits"]["max_depth"]
                              for state in states
                              if state["limits"]["max_depth"] is not None),
                             default=None),
        },
        "root": _merge_nodes([state["root"] for state in states]),
    }


def _node_to_schema(node):
    entries = []
    for key in _TYPE_ORDER:
        if key not in node["types"]:
            continue
        entry = {'type': ['null', key]}
        if key == 'object':
            # Sorted field names keep the merged schema deterministic
            # regardless of observation and merge order.
            entry['properties'] = {
                name: _node_to_schema(node["fields"][name]["node"])
                for name in sorted(node["fields"])
            }
        elif key == 'array':
            if node["items"] is not None:
                entry['items'] = _node_to_schema(node["items"])
            # No elements observed: leave items unconstrained rather than
            # claiming they can only be null.
        elif key == 'number':
            # Use type=string, format=singer.decimal
            entry['type'] = ['null', 'string']
            entry['format'] = 'singer.decimal'
        entries.append(entry)

    if not entries:
        if node["nulls"]:
            return {'type': ['null']}
        return {'type': ['null', 'string']}
    if len(entries) == 1:
        return entries[0]
    return {'anyOf': entries}


def finalize(state, allow_incomplete=False):
    """Derive a conservative Singer schema from an observation state.

    Only conclusions supported by the observed samples are declared: types
    are limited to what was seen, object fields are never required (a field
    may be absent from other samples), and arrays with no observed elements
    carry no items constraint. Raises ``IncompleteObservationError`` if the
    state hit observation limits, unless ``allow_incomplete`` is set.
    """
    _check_state(state)
    if not state.get("complete", False) and not allow_incomplete:
        raise IncompleteObservationError(
            "Observation state is incomplete (field or depth limits were "
            "hit); pass allow_incomplete=True to finalize anyway")
    return _node_to_schema(state["root"])


def add_observation(acc, path):

    node = acc
    for i in range(0, len(path) - 1):
        k = path[i]
        if k not in node:
            node[k] = {}
        node = node[k]

    node[path[-1]] = True

# pylint: disable=too-many-branches
def add_observations(acc, path, data):
    if isinstance(data, dict):
        for key in data:
            add_observations(acc, path + ["object", key], data[key])
    elif isinstance(data, list):
        if len(data) == 0:
            add_observations(acc, path + ["array"], None)
        for item in data:
            add_observations(acc, path + ["array"], item)
    elif isinstance(data, str):
        try:
            # If the string parses as a int, add an observation that it's a integer
            int(data)
            add_observation(acc, path + ["integer"])
            return acc
        except (ValueError, TypeError):
            pass
        try:
            # If the string parses as a float, add an observation that it's a number
            float(data)
            add_observation(acc, path + ["number"])
            return acc
        except (ValueError, TypeError):
            pass
        add_observation(acc, path + ["string"])
    elif isinstance(data, bool):
        add_observation(acc, path + ["boolean"])
    elif isinstance(data, int):
        add_observation(acc, path + ["integer"])
    elif isinstance(data, float):
        add_observation(acc, path + ["number"])
    elif data is None:
        add_observation(acc, path + ["null"])
    else:
        add_observation(acc, path + ["string"])

    return acc

def to_json_schema(obs):
    types = []
    # add schema types in a specific order to anyOf list
    for key in ['array', 'object', 'number', 'integer', 'boolean', 'string', 'null']:
        if key not in obs:
            continue

        result = {'type': ['null']}

        if key == 'object':
            result['type'] += ['object']
            if 'properties' not in result:
                result['properties'] = {}
                for obj_key in obs['object']:
                    result['properties'][obj_key] = to_json_schema(obs['object'][obj_key])

        elif key == 'array':
            result['type'] += ['array']
            result['items'] = to_json_schema(obs['array'])

        elif key == 'string':
            result['type'] += ['string']

        elif key == 'boolean':
            result['type'] += ['boolean']

        elif key == 'integer':
            result['type'] += ['integer']

        elif key == 'number':
            # Use type=string, format=singer.decimal
            result['type'] += ['string']
            result['format'] = 'singer.decimal'

        elif key == 'null':
            pass

        else:
            raise Exception("Unexpected data type " + key)

        types.append(result)

    if len(types) == 0:
        return {'type': ['null', 'string']}

    if len(types) == 1:
        return types[0]

    return {'anyOf': types}

def generate_schema(records):
    obs = {}
    for record in records:
        obs = add_observations(obs, [], record)
    return to_json_schema(obs)
