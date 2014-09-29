# Representing literals

## Design space
* In-memory vs wire representation
* Encoding in bytecode
* Representation of literal nodes (explicit nodes vs calls to type constructors)
* Primordial literal types

## Some ideas

### Option 1
* Literals are represented as calls to type constructor
* Only one primordial type: byte[]
* Wire representation: any reasonable encoding of byte[] (e.g., hex, base64, etc)
* Embedding in bytecode done via invokedynamic with constant callsite

#### Examples

    BigInt(byte[] { ... })
    IPV6(byte[] { ... })

#### Trade-offs
* Wire representation not human-friendly
* Can't use LDC for simple types (boolean, long, double) when generating bytecode
** Maybe special-case this in the bytecode compiler?
* Requires parsing bytes when interpreting expressions or binding constant callsite
* Can make bytecode transportable by encoding as java Strings in constant pool (it doesn't depend on function binding map)

### Option 2
* Similar to Option 1, but support for multiple primordial types with Java semantics: byte[], boolean, int, long, float, double, etc.

#### Trade-offs
* Requires parsing/assembling complex objects when interpreting expression or binding constant callsite

#### Examples

    BigInt(1234567)
    Varchar('abcdef')
    Double(123.456)
    IPV6(12345, 6789)

### Option 3
* Similar to Option 1, but string as the primordial type

#### Examples

    BigInt('123456')
    Varchar('abcdef')
    Double('123.456)
    IPV6('fd4b:f160:ee60:d50e:d065:cda4:5dc1:7b59')
    Timestamp(123456.789)

#### Trade-offs
* Wire format is human-readable
* Requires parsing string when interpreting expressions or binding constant callsite


### Option 4

* Explicit Literal node with value in native in-memory format (e.g., IPV6 might be a byte[], Bigint a Long, etc)
* Wire format as either string or byte[]
* Embedding in bytecode via invokedynamic and constant callsite

#### Tradeoffs
* No need to parse/assemble values when they need to be interpreted
* Mismatch between in-memory representation and wire-format

    e.g., for BigInt

        Long(1234)

        vs.

        Literal("bigint", "1234")

* Can use LDC for embedding "simple" types in bytecode
* Bytecode cannot be distributed without the invokedynamic bindings (this is also true of current function binder mechanism)

## Likely candidate

* Types declare a type constructor and a serializer (type <-> string). Note that this is not the same as CAST!
* Explicit Literal node. Value is stored in native in-memory format for the type
* Wire format encodes literals as string
* Embedding in bytecode via invokedynamic and constant callsite for complex types, LDC for simple types that can be mapped to Java primitives.
* Null constants represented as Literal node with "null" and appropriate type. If type is unknown, UnknownType.UNKNOWN
