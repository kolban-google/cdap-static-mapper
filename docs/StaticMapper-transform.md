# Static mapper transform

Description
-----------

Maps one or more fields in the incoming data to new value in the outgoing data by performing
a lookup of a statically supplied JSON Document.

Use Case
--------

This transform is used whenever you need to map fields in incoming data to new outgoing data by
performing a lookup/mapping using static values.

As an example, imagine we have incoming data that contains US state names such as "Texas" or "California" and
we wish the output data to contain state codes such as "TX" or "CA".  We can supply the mapping data as a
JSON string which is an array of objects.  For example:

```
[
    {
        "name": "Texas",
        "code": "TX
    },
    {
        "name": "California",
        "code": "CA"
    },
    ....
]
```

In order to perform the mapping, we next specify which field in the incoming data should be mapped and which field in the JSON object
should be used as a key and which as a value.  For example:

```
field: state
key: name
value: code
```

would replace the incoming field called `state` in the input message with the value of the JSON document field called `code` where the JSON document
field called `name` would be used to find the correct object.



Properties
----------

**json_data:** The JSON data that contains our mapping.

**mappings:** Delimited data defining the mappings.  Each mapping is given by `field:key:value:default` and mappings are comma delimited.

Example
-------

This example lowercases the 'name' field and uppercases the 'id' field:

    {
        "name": "static-mapper",
        "type": "transform",
        "properties": {
            "json_data": "<JSON DOC>",
            "mappings": "<Mapppings>"
        }
    }
