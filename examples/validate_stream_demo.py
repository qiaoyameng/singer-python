"""Demo for singer.validation.MessageStreamValidator.

Run from the repository root:

    PYTHONUTF8=1 python examples/validate_stream_demo.py

The stream below interleaves two streams, changes the ``users`` schema
once, and includes an ACTIVATE_VERSION that references an undeclared
version.  No target or database is involved: every event is produced in
memory, in input order.
"""

import singer
from singer.validation import MessageStreamValidator

USERS_V1 = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
    },
    "required": ["id"],
}

# V2 makes email mandatory; records accepted under V1 keep their verdict.
USERS_V2 = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "email": {"type": "string"},
    },
    "required": ["id", "email"],
}

ORDERS = {
    "type": "object",
    "properties": {
        "order_id": {"type": "integer"},
        "total": {"type": "number"},
    },
    "required": ["order_id"],
}

LINE_STREAM = [
    singer.format_message(singer.SchemaMessage(
        stream="users", schema=USERS_V1, key_properties=["id"])),
    singer.format_message(singer.SchemaMessage(
        stream="orders", schema=ORDERS, key_properties=["order_id"])),
    singer.format_message(singer.RecordMessage(
        stream="users", record={"id": 1, "name": "Ada"}, version=100)),
    singer.format_message(singer.RecordMessage(
        stream="orders", record={"order_id": 55, "total": 9.99})),
    singer.format_message(singer.ActivateVersionMessage(
        stream="users", version=999)),
    singer.format_message(singer.ActivateVersionMessage(
        stream="users", version=100)),
    singer.format_message(singer.SchemaMessage(
        stream="users", schema=USERS_V2, key_properties=["id"])),
    singer.format_message(singer.RecordMessage(
        stream="users", record={"id": 2, "name": "Grace"}, version=100)),
    singer.format_message(singer.RecordMessage(
        stream="users",
        record={"id": 3, "name": "Linus", "email": "linus@example.com"},
        version=100)),
    singer.format_message(singer.StateMessage(
        value={"bookmarks": {"users": 3, "orders": 55}})),
]


def main():
    print("== collect mode: errors are reported, state stays consistent ==")
    validator = MessageStreamValidator(
        stream_version_policies={"users": singer.VersionPolicy.ALLOW})
    for event in validator.validate(LINE_STREAM):
        print("seq={:<2} {}".format(event.sequence, event))

    print("\ntotal errors collected: {}".format(validator.total_errors))

    print("\n== stop mode: the first failure raises ==")
    strict = MessageStreamValidator(collect_errors=False)
    try:
        list(strict.validate(LINE_STREAM))
    except singer.MessageValidationError as exc:
        print("raised on seq {}: {}".format(
            exc.event.sequence, exc.event))


if __name__ == "__main__":
    main()
