//! Finite offline Draft2020-12 profile for the production jsonschema validator.
use jsonschema::{Draft, Resource, Retrieve, Uri, Validator};
use partitionline_schema::json_schema::{Codec, Limits, Schema, Selection, DIALECT_URI};
use serde_json::Value;
use std::{collections::HashMap, io::Write};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Json,
    Schema,
    MissingReference,
    CyclicReference,
    ProfileLimit,
    Instance,
    Output,
}
impl std::fmt::Display for Failure {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(out, "{self:?}")
    }
}
impl std::error::Error for Failure {}

struct DenyRetrieval;
impl Retrieve for DenyRetrieval {
    fn retrieve(&self, _: &Uri<String>) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("only explicitly supplied offline resources are permitted".into())
    }
}

fn bound_value(value: &Value, depth: usize, nodes: &mut usize) -> Result<(), Failure> {
    *nodes += 1;
    if depth > 16 || *nodes > 512 {
        return Err(Failure::ProfileLimit);
    }
    match value {
        Value::Array(values) => {
            if values.len() > 32 {
                return Err(Failure::ProfileLimit);
            }
            for value in values {
                bound_value(value, depth + 1, nodes)?;
            }
        }
        Value::Object(values) => {
            if values.len() > 32 {
                return Err(Failure::ProfileLimit);
            }
            if values.keys().any(|key| key.len() > 128) {
                return Err(Failure::ProfileLimit);
            }
            for value in values.values() {
                bound_value(value, depth + 1, nodes)?;
            }
        }
        Value::String(value) if value.len() > 4091 => return Err(Failure::ProfileLimit),
        _ => {}
    }
    Ok(())
}

fn parse(text: &str) -> Result<Value, Failure> {
    if text.len() > 16 * 1024 {
        return Err(Failure::ProfileLimit);
    }
    let value: Value = serde_json::from_str(text).map_err(|_| Failure::Json)?;
    bound_value(&value, 0, &mut 0)?;
    Ok(value)
}

// The selected numeric family avoids precision loss/underflow in an IEEE754
// parser. Integers are JSON integer literals. Floating spellings have at most
// eight fractional digits, exponent -6..6, and mantissa/result magnitude <=1e6. Other numeric
// spellings fail explicitly; arbitrary decimal semantics are not qualified.
fn bound_numbers(text: &str) -> Result<(), Failure> {
    let bytes = text.as_bytes();
    let (mut i, mut quoted, mut escaped) = (0, false, false);
    while i < bytes.len() {
        let byte = bytes[i];
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            i += 1;
            continue;
        }
        if byte == b'"' {
            quoted = true;
            i += 1;
            continue;
        }
        if byte == b'-' || byte.is_ascii_digit() {
            let start = i;
            i += 1;
            while i < bytes.len()
                && (bytes[i].is_ascii_digit()
                    || matches!(bytes[i], b'.' | b'e' | b'E' | b'+' | b'-'))
            {
                i += 1;
            }
            let token = &text[start..i];
            if token.contains(['.', 'e', 'E']) {
                let (mantissa, exponent) = token.split_once(['e', 'E']).unwrap_or((token, "0"));
                let exponent: i32 = exponent.parse().map_err(|_| Failure::ProfileLimit)?;
                let decimals = mantissa.split_once('.').map_or(0, |(_, rest)| rest.len());
                let number: f64 = token.parse().map_err(|_| Failure::Json)?;
                let mantissa: f64 = mantissa.parse().map_err(|_| Failure::Json)?;
                if !(-6..=6).contains(&exponent)
                    || decimals > 8
                    || mantissa.abs() > 1_000_000.0
                    || !number.is_finite()
                    || number.abs() > 1_000_000.0
                {
                    return Err(Failure::ProfileLimit);
                }
            } else if token.parse::<i64>().is_err() {
                return Err(Failure::ProfileLimit);
            }
            continue;
        }
        i += 1;
    }
    Ok(())
}

fn inspect_schema(
    value: &Value,
    resources: &HashMap<String, Value>,
    stack: &mut Vec<String>,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), Failure> {
    *nodes += 1;
    if depth > 16 || *nodes > 512 {
        return Err(Failure::ProfileLimit);
    }
    if value.is_boolean() {
        return Ok(());
    }
    let object = value.as_object().ok_or(Failure::Schema)?;
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "$schema"
                | "$id"
                | "$ref"
                | "type"
                | "anyOf"
                | "properties"
                | "required"
                | "additionalProperties"
                | "minimum"
                | "maximum"
                | "enum"
                | "const"
                | "default"
                | "format"
                | "title"
                | "description"
        ) {
            return Err(Failure::ProfileLimit);
        }
    }
    if object
        .get("$schema")
        .is_some_and(|uri| uri.as_str() != Some(DIALECT_URI))
    {
        return Err(Failure::Schema);
    }
    if let Some(reference) = object.get("$ref") {
        let uri = reference.as_str().ok_or(Failure::Schema)?;
        if stack.iter().any(|seen| seen == uri) {
            return Err(Failure::CyclicReference);
        }
        let resource = resources.get(uri).ok_or(Failure::MissingReference)?;
        stack.push(uri.into());
        inspect_schema(resource, resources, stack, depth + 1, nodes)?;
        stack.pop();
    }
    if let Some(properties) = object.get("properties") {
        for schema in properties.as_object().ok_or(Failure::Schema)?.values() {
            inspect_schema(schema, resources, stack, depth + 1, nodes)?;
        }
    }
    if let Some(branches) = object.get("anyOf") {
        let branches = branches.as_array().ok_or(Failure::Schema)?;
        if branches.is_empty() || branches.len() > 4 {
            return Err(Failure::ProfileLimit);
        }
        for schema in branches {
            inspect_schema(schema, resources, stack, depth + 1, nodes)?;
        }
    }
    if let Some(schema) = object.get("additionalProperties") {
        inspect_schema(schema, resources, stack, depth + 1, nodes)?;
    }
    Ok(())
}

fn validator(selection: Schema<'_>) -> Result<Validator, Failure> {
    bound_numbers(selection.json)?;
    let schema = parse(selection.json)?;
    let mut resources = HashMap::new();
    for reference in selection.references {
        bound_numbers(reference.json)?;
        let resource = parse(reference.json)?;
        // The finite family has one exact URI per resource, without aliases,
        // pointer fragments, dynamic references, patterns or recursion.
        if resource.get("$id").and_then(Value::as_str) != Some(reference.uri) {
            return Err(Failure::Schema);
        }
        resources.insert(reference.uri.to_owned(), resource);
    }
    inspect_schema(&schema, &resources, &mut Vec::new(), 0, &mut 0)?;
    for resource in resources.values() {
        inspect_schema(resource, &resources, &mut Vec::new(), 0, &mut 0)?;
    }
    let mut options = jsonschema::options()
        .with_draft(Draft::Draft202012)
        .with_retriever(DenyRetrieval)
        .should_validate_formats(false);
    for (uri, value) in resources {
        options = options.with_resource(
            uri,
            Resource::from_contents(value).map_err(|_| Failure::Schema)?,
        );
    }
    options.build(&schema).map_err(|_| Failure::Schema)
}

#[derive(Default)]
pub struct SelectedJsonSchema {
    writer: Option<Validator>,
    reader: Option<Validator>,
}
struct Count(usize);
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Codec for SelectedJsonSchema {
    type Value = Value;
    type Error = Failure;
    fn resolve(
        &mut self,
        writer: Schema<'_>,
        reader: Schema<'_>,
        _: Limits,
    ) -> Result<(), Failure> {
        self.writer = Some(validator(writer)?);
        self.reader = Some(validator(reader)?);
        Ok(())
    }
    fn encoded_len(&self, value: &Value) -> Result<usize, Failure> {
        bound_value(value, 0, &mut 0)?;
        let mut output = Count(0);
        serde_json::to_writer(&mut output, value).map_err(|_| Failure::Output)?;
        Ok(output.0)
    }
    fn encode_into(&self, value: &Value, mut output: &mut [u8]) -> Result<usize, Failure> {
        let length = output.len();
        serde_json::to_writer(&mut output, value).map_err(|_| Failure::Output)?;
        Ok(length - output.len())
    }
    fn validate(&self, text: &str, selection: Selection) -> Result<(), Failure> {
        let value = parse(text)?;
        bound_numbers(text)?;
        let validator = match selection {
            Selection::Writer => &self.writer,
            Selection::Reader => &self.reader,
        }
        .as_ref()
        .ok_or(Failure::Schema)?;
        if validator.is_valid(&value) {
            Ok(())
        } else {
            Err(Failure::Instance)
        }
    }
    fn decode(&self, text: &str) -> Result<(Value, usize), Failure> {
        Ok((parse(text)?, text.len()))
    }
}
