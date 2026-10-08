//! Apache Avro 0.22.0 bridge for the finite offline conformance profile.
//! This is test support, not a serializer selected by the library.
use apache_avro::{
    reader::datum::GenericDatumReader,
    schema::{Name, NamesRef, ResolvedSchema},
    types::Value,
    writer::datum::GenericDatumWriter,
    Schema as AvroSchema,
};
use partitionline_schema::avro::{Codec, Limits, Schema};
use std::io::{self, Cursor, Read, Write};

pub const ALLOCATION_LIMIT: usize = 4096;
const MAX_DEPTH: usize = 8;
const MAX_NODES: usize = 128;

#[derive(Debug)]
pub enum Failure {
    Policy(&'static str),
    Avro(apache_avro::Error),
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Keep schemas, field values and backend diagnostics out of errors.
        match self {
            Self::Policy(reason) => write!(f, "selected Avro policy: {reason}"),
            Self::Avro(_) => f.write_str("selected Avro codec failure"),
        }
    }
}
impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Avro(error) => Some(error),
            Self::Policy(_) => None,
        }
    }
}
impl From<apache_avro::Error> for Failure {
    fn from(error: apache_avro::Error) -> Self {
        Self::Avro(error)
    }
}

struct Selection {
    root: AvroSchema,
    references: Vec<AvroSchema>,
}
impl Selection {
    fn schemata(&self) -> Vec<&AvroSchema> {
        // The production name resolver accepts this dependency-first list.
        self.references.iter().chain([&self.root]).collect()
    }

    fn parse(input: Schema<'_>) -> Result<Self, Failure> {
        for json in std::iter::once(input.json).chain(input.references.iter().map(|r| r.json)) {
            check_json_depth(json)?;
        }
        let (root, references) = AvroSchema::parse_str_with_list(
            input.json,
            input.references.iter().map(|reference| reference.json),
        )?;
        for (reference, parsed) in input.references.iter().zip(&references) {
            if parsed.name() != Some(&Name::new(reference.name)?) {
                return Err(Failure::Policy("reference full name mismatch"));
            }
        }
        let selected = Self { root, references };
        let resolved = ResolvedSchema::new_with_schemata(selected.schemata())?;
        let mut nodes = 0;
        for schema in selected.schemata() {
            check_schema(schema, resolved.get_names(), 0, &mut nodes)?;
        }
        Ok(selected)
    }
}

// Scan before any JSON parser. Strings and escaped delimiters do not add depth.
fn check_json_depth(json: &str) -> Result<(), Failure> {
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for byte in json.bytes() {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else {
            match byte {
                b'"' => quoted = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > 32 {
                        return Err(Failure::Policy("schema JSON nesting"));
                    }
                }
                b'}' | b']' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    Ok(()) // The production parser checks JSON syntax.
}

fn budget(depth: usize, nodes: &mut usize) -> Result<(), Failure> {
    *nodes += 1;
    if depth > MAX_DEPTH || *nodes > MAX_NODES {
        return Err(Failure::Policy("expanded depth or node count"));
    }
    Ok(())
}

fn check_schema(
    schema: &AvroSchema,
    names: &NamesRef<'_>,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), Failure> {
    budget(depth, nodes)?;
    match schema {
        AvroSchema::Null
        | AvroSchema::Boolean
        | AvroSchema::Int
        | AvroSchema::Long
        | AvroSchema::Float
        | AvroSchema::Double
        | AvroSchema::String
        | AvroSchema::Bytes => {}
        AvroSchema::Record(record) if record.fields.len() <= 32 => {
            for field in &record.fields {
                check_schema(&field.schema, names, depth + 1, nodes)?;
            }
        }
        AvroSchema::Union(union) if union.variants().len() <= 4 => {
            for variant in union.variants() {
                check_schema(variant, names, depth + 1, nodes)?;
            }
        }
        AvroSchema::Ref { name } => {
            let target = names
                .get(name)
                .ok_or(Failure::Policy("unresolved reference"))?;
            // Cycles fail the finite depth budget before any datum operation.
            check_schema(target, names, depth + 1, nodes)?;
        }
        _ => return Err(Failure::Policy("schema outside finite profile")),
    }
    Ok(())
}

fn check_value(value: &Value) -> Result<(), Failure> {
    fn visit(
        value: &Value,
        depth: usize,
        nodes: &mut usize,
        bytes: &mut usize,
    ) -> Result<(), Failure> {
        budget(depth, nodes)?;
        match value {
            Value::Null
            | Value::Boolean(_)
            | Value::Int(_)
            | Value::Long(_)
            | Value::Float(_)
            | Value::Double(_) => {}
            Value::String(value) => *bytes += value.len(),
            Value::Bytes(value) => *bytes += value.len(),
            Value::Record(fields) if fields.len() <= 32 => {
                for (_, value) in fields {
                    visit(value, depth + 1, nodes, bytes)?;
                }
            }
            Value::Union(_, value) => visit(value, depth + 1, nodes, bytes)?,
            _ => return Err(Failure::Policy("value outside finite profile")),
        }
        if *bytes > ALLOCATION_LIMIT {
            return Err(Failure::Policy("aggregate scalar bytes"));
        }
        Ok(())
    }
    visit(value, 0, &mut 0, &mut 0)
}

#[derive(Default)]
pub struct SelectedAvro {
    writer: Option<Selection>,
    reader: Option<Selection>,
    payload_limit: usize,
}
impl SelectedAvro {
    fn writer(&self) -> Result<GenericDatumWriter<'_>, Failure> {
        let writer = self
            .writer
            .as_ref()
            .ok_or(Failure::Policy("not resolved"))?;
        Ok(GenericDatumWriter::builder(&writer.root)
            .schemata(writer.schemata())?
            .build()?)
    }
}

// The pinned backend can suppress UnexpectedEof for strings. Keep that error
// observable even if schema resolution subsequently turns the value into null.
struct StrictInput<'a> {
    cursor: Cursor<&'a [u8]>,
    truncated: bool,
}
impl Read for StrictInput<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.cursor.read(output)
    }
    fn read_exact(&mut self, output: &mut [u8]) -> io::Result<()> {
        let result = self.cursor.read_exact(output);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == io::ErrorKind::UnexpectedEof)
        {
            self.truncated = true;
        }
        result
    }
}

struct CountSink {
    count: usize,
    limit: usize,
}
impl Write for CountSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self
            .count
            .checked_add(bytes.len())
            .filter(|count| *count <= self.limit)
            .ok_or_else(|| io::Error::other("selected Avro payload limit"))?;
        self.count = count;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Codec for SelectedAvro {
    type Value = Value;
    type Error = Failure;
    fn resolve(
        &mut self,
        writer: Schema<'_>,
        reader: Schema<'_>,
        limits: Limits,
    ) -> Result<(), Failure> {
        // OnceLock in the backend: refuse a process with a different established guard.
        if apache_avro::util::max_allocation_bytes(ALLOCATION_LIMIT) != ALLOCATION_LIMIT {
            return Err(Failure::Policy(
                "backend allocation guard already configured",
            ));
        }
        if limits.max_frame_bytes() > ALLOCATION_LIMIT
            || limits.max_schema_bytes() > 16 * 1024
            || limits.max_references() > 8
        {
            return Err(Failure::Policy("input limits outside finite profile"));
        }
        self.writer = Some(Selection::parse(writer)?);
        self.reader = Some(Selection::parse(reader)?);
        self.payload_limit = limits.max_frame_bytes() - 5;
        Ok(())
    }
    fn encoded_len(&self, value: &Value) -> Result<usize, Failure> {
        check_value(value)?;
        let mut sink = CountSink {
            count: 0,
            limit: self.payload_limit,
        };
        self.writer()?.write_value_ref(&mut sink, value)?;
        Ok(sink.count)
    }
    fn encode_into(&self, value: &Value, output: &mut [u8]) -> Result<usize, Failure> {
        check_value(value)?;
        if output.len() > self.payload_limit {
            return Err(Failure::Policy("output length"));
        }
        let mut cursor = Cursor::new(output);
        self.writer()?.write_value_ref(&mut cursor, value)?;
        Ok(cursor.position() as usize)
    }
    fn decode(&self, payload: &[u8]) -> Result<(Value, usize), Failure> {
        if payload.len() > self.payload_limit {
            return Err(Failure::Policy("input length"));
        }
        let writer = self
            .writer
            .as_ref()
            .ok_or(Failure::Policy("not resolved"))?;
        let reader = self
            .reader
            .as_ref()
            .ok_or(Failure::Policy("not resolved"))?;
        let codec = GenericDatumReader::builder(&writer.root)
            .writer_schemata(writer.schemata())?
            .reader_schema(&reader.root)
            .reader_schemata(reader.schemata())?
            .build()?;
        let mut input = StrictInput {
            cursor: Cursor::new(payload),
            truncated: false,
        };
        let decoded = codec.read_value(&mut input);
        if input.truncated {
            return Err(Failure::Policy("truncated datum"));
        }
        let value = decoded?;
        // This is an acceptance bound after decode, not an RSS/allocation guard.
        check_value(&value)?;
        Ok((value, input.cursor.position() as usize))
    }
}

pub fn json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(value) => (*value).into(),
        Value::Int(value) => (*value).into(),
        Value::Long(value) => (*value).into(),
        Value::Float(value) => (*value).into(),
        Value::Double(value) => (*value).into(),
        Value::String(value) => value.clone().into(),
        Value::Union(_, value) => json(value),
        Value::Record(fields) => fields
            .iter()
            .map(|(name, value)| (name.clone(), json(value)))
            .collect(),
        _ => panic!("comparison outside selected corpus"),
    }
}
