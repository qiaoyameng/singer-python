singer-python
===================

Writes the Singer format from Python

Use
---

This library depends on python3. We recommend using a `virtualenv`
like this:

```bash
python3 -m venv ~/.virtualenvs/singer-python
```

Next, install this library:

```bash
source ~/.virtualenvs/singer-python/bin/activate
git clone http://github.com/singer-io/singer-python
cd singer-python
make install
```

Now, from python code within the same `virtualenv`, you can use the
library:

```python
import singer

singer.write_schema('my_table',
	            {'properties':{'id': {'type': 'string', 'key': True}}},
		    ['id'])
singer.write_records('my_table',
                     [{'id': 'b'}, {'id':'d'}])
singer.write_state({'my_table': 'd'})
```

Flattening array-of-object fields
---------------------------------

Some targets only accept flat tables. `singer.array_flattener` is a message
transformer that sits between a tap and a target and expands configured
top-level array-of-object properties into child streams. The parent stream
keeps every other field and gains one `<array>_array_status` column per
configured array whose value is one of `missing`, `null`, `empty` or
`present`, so the three empty-ish cases stay distinguishable.

Each child record carries the typed parent key property values and an
`_sdc_source_array_index` (its position in the original array). Arrays are
expanded independently in configuration order, never as a cartesian product,
and duplicate array elements are preserved. Child `SCHEMA` messages,
`key_properties` (`<parent keys> + _sdc_source_array_index`) and selection
metadata are generated automatically. A `STATE` message is only forwarded
after all messages derived from the preceding records have been written.

Configuration:

```json
{
  "streams": [
    {
      "stream": "users",
      "key_properties": ["id"],
      "arrays": [
        {"array_path": "tags"},
        {"array_path": "emails", "child_stream": "user_emails"}
      ]
    }
  ]
}
```

Use it as a library with an injectable writer, or as a CLI that reads Singer
JSON lines on stdin and writes the transformed stream to stdout:

```python
from singer import ArrayFlattener

flattener = ArrayFlattener(config, writer=output.append)
for message in parsed_tap_messages:
    flattener.handle_message(message)
```

```bash
tap-whatever | singer-array-flatten --config flattener-config.json | target-whatever
```


License
-------

Copyright © 2017 Stitch

Distributed under the Apache License Version 2.0
