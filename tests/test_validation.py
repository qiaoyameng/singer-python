import unittest

import singer
from singer import (
    ActivateVersionMessage,
    MessageStreamValidator,
    MessageValidationError,
    RecordMessage,
    SchemaMessage,
    StateMessage,
    ValidationErrorEvent,
    VersionPolicy,
)


USERS_SCHEMA_V1 = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
    },
    "required": ["id"],
}

USERS_SCHEMA_V2 = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "email": {"type": "string"},
    },
    "required": ["id", "email"],
}

ORDERS_SCHEMA = {
    "type": "object",
    "properties": {
        "order_id": {"type": "integer"},
        "total": {"type": "number"},
    },
    "required": ["order_id"],
}


def events_for(messages, **kwargs):
    validator = MessageStreamValidator(**kwargs)
    return list(validator.validate(messages)), validator


class TestMessageStreamValidator(unittest.TestCase):

    def test_record_before_schema_is_rejected(self):
        events, validator = events_for([
            RecordMessage(stream="users", record={"id": 1}),
            SchemaMessage(stream="users", schema=USERS_SCHEMA_V1,
                          key_properties=["id"]),
            RecordMessage(stream="users", record={"id": 2}),
        ])
        self.assertIsInstance(events[0], ValidationErrorEvent)
        self.assertEqual(events[0].sequence, 1)
        self.assertEqual(events[0].stream, "users")
        self.assertEqual(events[0].code, "missing_schema")
        self.assertIsInstance(events[1], singer.SchemaAccepted)
        self.assertIsInstance(events[2], singer.RecordAccepted)
        self.assertEqual(validator.total_errors, 1)

    def test_schema_change_only_affects_future_records(self):
        events, _ = events_for([
            SchemaMessage(stream="users", schema=USERS_SCHEMA_V1,
                          key_properties=["id"]),
            RecordMessage(stream="users", record={"id": 1}),
            SchemaMessage(stream="users", schema=USERS_SCHEMA_V2,
                          key_properties=["id"]),
            RecordMessage(stream="users", record={"id": 2}),
            RecordMessage(stream="users",
                          record={"id": 3, "email": "a@example.com"}),
        ])
        self.assertEqual(events[0].schema_generation, 1)
        self.assertIsInstance(events[1], singer.RecordAccepted)
        self.assertEqual(events[2].schema_generation, 2)
        # Same payload shape was accepted before the change; now it fails.
        self.assertEqual(events[3].code, "schema_violation")
        self.assertEqual(events[3].path, "email")
        self.assertIsInstance(events[4], singer.RecordAccepted)
        # The earlier accepted record keeps its conclusion.
        self.assertIsInstance(events[1], singer.RecordAccepted)

    def test_activate_version_must_reference_declared_version(self):
        events, _ = events_for([
            SchemaMessage(stream="users", schema=USERS_SCHEMA_V1,
                          key_properties=["id"]),
            ActivateVersionMessage(stream="users", version=7),
            RecordMessage(stream="users", record={"id": 1}, version=7),
            ActivateVersionMessage(stream="users", version=7),
        ])
        self.assertEqual(events[1].code, "undeclared_version")
        self.assertEqual(events[1].path, "version")
        self.assertIsInstance(events[2], singer.RecordAccepted)
        self.assertIsInstance(events[3], singer.VersionActivated)
        self.assertEqual(events[3].version, 7)

    def test_activate_version_before_schema(self):
        events, _ = events_for([
            ActivateVersionMessage(stream="users", version=1),
        ])
        self.assertEqual(events[0].code, "missing_schema")
        self.assertEqual(events[0].sequence, 1)

    def test_version_policies(self):
        events, _ = events_for(
            [
                SchemaMessage(stream="users", schema=USERS_SCHEMA_V1,
                              key_properties=["id"]),
                RecordMessage(stream="users", record={"id": 1}),
            ],
            version_policy=VersionPolicy.REQUIRE)
        self.assertEqual(events[1].code, "version_required")

        events, _ = events_for(
            [
                SchemaMessage(stream="users", schema=USERS_SCHEMA_V1,
                              key_properties=["id"]),
                RecordMessage(stream="users", record={"id": 1}, version=2),
                ActivateVersionMessage(stream="users", version=2),
            ],
            version_policy=VersionPolicy.FORBID)
        self.assertEqual(events[1].code, "version_forbidden")
        self.assertEqual(events[2].code, "version_forbidden")

        events, _ = events_for([
            SchemaMessage(stream="users", schema=USERS_SCHEMA_V1,
                          key_properties=["id"]),
            RecordMessage(stream="users", record={"id": 1}, version="2"),
        ])
        self.assertEqual(events[1].code, "invalid_version_type")
        self.assertEqual(events[1].path, "version")

    def test_failed_record_does_not_declare_version_or_mutate_state(self):
        events, validator = events_for([
            SchemaMessage(stream="users", schema=USERS_SCHEMA_V1,
                          key_properties=["id"]),
            RecordMessage(stream="users",
                          record={"id": "bad"}, version=9),
            ActivateVersionMessage(stream="users", version=9),
            RecordMessage(stream="users", record={"id": 2}, version=9),
            ActivateVersionMessage(stream="users", version=9),
        ])
        self.assertEqual(events[1].code, "schema_violation")
        # Version 9 was not declared by the rejected record.
        self.assertEqual(events[2].code, "undeclared_version")
        self.assertIsInstance(events[3], singer.RecordAccepted)
        self.assertIsInstance(events[4], singer.VersionActivated)
        state = validator._streams["users"]
        self.assertEqual(state.records_accepted, 1)
        self.assertEqual(state.records_rejected, 1)
        self.assertEqual(state.declared_versions, {9})

    def test_stop_mode_raises_on_first_error(self):
        validator = MessageStreamValidator(collect_errors=False)
        with self.assertRaises(MessageValidationError) as ctx:
            list(validator.validate([
                RecordMessage(stream="users", record={"id": 1}),
            ]))
        self.assertEqual(ctx.exception.event.code, "missing_schema")
        self.assertEqual(ctx.exception.event.sequence, 1)

    def test_state_checkpoint_reports_counts_not_commitment(self):
        events, _ = events_for([
            SchemaMessage(stream="users", schema=USERS_SCHEMA_V1,
                          key_properties=["id"]),
            SchemaMessage(stream="orders", schema=ORDERS_SCHEMA,
                          key_properties=["order_id"]),
            RecordMessage(stream="users", record={"id": 1}),
            RecordMessage(stream="orders",
                          record={"order_id": "oops"}),
            StateMessage(value={"bookmarks": {"users": 1}}),
        ])
        checkpoint = events[-1]
        self.assertIsInstance(checkpoint, singer.CheckpointEvent)
        self.assertIsNone(checkpoint.stream)
        self.assertEqual(checkpoint.sequence, 5)
        self.assertEqual(checkpoint.value,
                         {"bookmarks": {"users": 1}})
        self.assertEqual(checkpoint.total_errors, 1)
        users = checkpoint.stream_counts["users"]
        orders = checkpoint.stream_counts["orders"]
        self.assertEqual(users["records_accepted"], 1)
        self.assertEqual(users["schema_generation"], 1)
        self.assertEqual(orders["records_rejected"], 1)
        self.assertEqual(orders["errors"], 1)

    def test_raw_lines_sequences_and_field_paths(self):
        lines = [
            '{"type": "SCHEMA", "stream": "users", '
            '"schema": {"type": "object", "properties": '
            '{"id": {"type": "integer"}, '
            '"tags": {"type": "array", '
            '"items": {"type": "integer"}}}}, '
            '"key_properties": ["id"]}',
            "",
            "{not valid json",
            '{"type": "WAT"}',
            '{"type": "RECORD", "stream": "users", '
            '"record": {"id": "x", "tags": [1, "bad"]}}',
        ]
        events, validator = events_for(lines)
        # Blank line is skipped and does not consume a sequence number.
        self.assertEqual(
            [event.sequence for event in events], [1, 2, 3, 4, 4])
        self.assertEqual(events[1].code, "invalid_message")
        self.assertIsNone(events[1].stream)
        self.assertEqual(events[2].code, "unknown_message_type")
        violations = {event.path: event for event in events[3:]}
        self.assertIn("id", violations)
        self.assertIn("tags[1]", violations)
        self.assertEqual(len(events[3:]), 2)
        for event in events[3:]:
            self.assertEqual(event.stream, "users")
            self.assertEqual(event.sequence, 4)
        # Two malformed lines plus two schema violations on the last record.
        self.assertEqual(validator.total_errors, 4)

    def test_remote_ref_is_rejected_without_network(self):
        events, validator = events_for([
            SchemaMessage(
                stream="users",
                schema={"type": "object",
                        "$ref": "https://example.com/remote.json"},
                key_properties=["id"]),
            RecordMessage(stream="users", record={"id": 1}),
        ])
        self.assertEqual(events[0].code, "remote_reference")
        # Rejected schema never became effective.
        self.assertEqual(events[1].code, "missing_schema")
        self.assertIsNone(validator._streams["users"].validator)

    def test_local_ref_still_resolves_in_process(self):
        schema = {
            "type": "object",
            "definitions": {
                "addr": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"],
                }
            },
            "properties": {
                "id": {"type": "integer"},
                "addr": {"$ref": "#/definitions/addr"},
            },
            "required": ["id"],
        }
        events, _ = events_for([
            SchemaMessage(stream="users", schema=schema,
                          key_properties=["id"]),
            RecordMessage(stream="users",
                          record={"id": 1, "addr": {"city": "Berlin"}}),
            RecordMessage(stream="users",
                          record={"id": 2, "addr": {}}),
        ])
        self.assertIsInstance(events[0], singer.SchemaAccepted)
        self.assertIsInstance(events[1], singer.RecordAccepted)
        self.assertEqual(events[2].code, "schema_violation")
        self.assertEqual(events[2].path, "addr.city")

    def test_two_streams_interleave_independently_and_in_order(self):
        messages = [
            SchemaMessage(stream="users", schema=USERS_SCHEMA_V1,
                          key_properties=["id"]),
            SchemaMessage(stream="orders", schema=ORDERS_SCHEMA,
                          key_properties=["order_id"]),
            RecordMessage(stream="users", record={"id": 1}),
            RecordMessage(stream="orders", record={"order_id": 10}),
            RecordMessage(stream="users", record={"id": 2}),
            ActivateVersionMessage(stream="orders", version=3),
        ]
        events, _ = events_for(messages)
        streams = [
            event.stream for event in events
            if getattr(event, "stream", None) is not None
        ]
        self.assertEqual(
            streams, ["users", "orders", "users", "orders",
                      "users", "orders"])
        self.assertEqual(events[-1].code, "undeclared_version")

    def test_convenience_helper_returns_events(self):
        events = singer.validate_message_stream([
            SchemaMessage(stream="users", schema=USERS_SCHEMA_V1,
                          key_properties=["id"]),
            RecordMessage(stream="users", record={"id": 1}),
        ])
        self.assertEqual(len(events), 2)
        self.assertIsInstance(events[0], singer.SchemaAccepted)
        self.assertIsInstance(events[1], singer.RecordAccepted)


if __name__ == "__main__":
    unittest.main()
