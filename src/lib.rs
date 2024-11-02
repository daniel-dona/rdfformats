use std::io::{BufReader, BufWriter, Write};

use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use oxrdfio::{RdfFormat, RdfParser, RdfSerializer};

/// Formats the sum of two numbers as string.
#[pyfunction]
fn convert(from_file: &str, to_file: &str, from_format: &str, to_format: &str) -> PyResult<u64> {
    let from_format_value = match from_format {
        "ttl" => RdfFormat::Turtle,
        "nt" => RdfFormat::NTriples,
        "nq" => RdfFormat::NQuads,
        "n3" => RdfFormat::N3,
        "trig" => RdfFormat::TriG,
        "rdf/xml" => RdfFormat::RdfXml, 
        _ => return Err(PyValueError::new_err(format!("Wrong value for format: {from_format}"))),
    };

    let to_format_value = match to_format {
        "ttl" => RdfFormat::Turtle,
        "nt" => RdfFormat::NTriples,
        "nq" => RdfFormat::NQuads,
        "n3" => RdfFormat::N3,
        "trig" => RdfFormat::TriG,
        "rdf/xml" => RdfFormat::RdfXml, 
        _ => return Err(PyValueError::new_err(format!("Wrong value for format: {}", from_format))),
    };

    // Open input and output files
    let from_file = std::fs::File::open(from_file)?;
    let to_file = std::fs::File::create(to_file)?;

    // Create a buffered reader for the input file
    let mut reader = BufReader::with_capacity(1024 * 1024, from_file);
    
    // Create a buffered writer for the output file
    let mut writer = BufWriter::with_capacity(1024 * 1024, to_file);

    // Streaming parser setup (assuming RdfParser supports streaming)
    let parser = RdfParser::from_format(from_format_value).for_reader(&mut reader);

    // Streaming serializer setup
    let mut serializer = RdfSerializer::from_format(to_format_value).for_writer(&mut writer);

    // Count triples/quads
    let mut quad_count: u64 = 0;

    // Process the file streamingly
    for result in parser {
        let quad = result.map_err(|e| {
            PyRuntimeError::new_err(format!("Parsing error: {e}"))
        })?;
        
        serializer.serialize_quad(&quad).map_err(|e| {
            PyRuntimeError::new_err(format!("Serialization error: {e}"))
        })?;

        quad_count += 1;
    }

    if let Err(e) = serializer.finish() {
        return Err(PyIOError::new_err(format!("Failed to flush serializer: {e}")));
    }

    // Ensure the writer is flushed (if necessary, depending on the library's behavior)
    if let Err(e) = writer.flush() {
        return Err(PyIOError::new_err(format!("Failed to flush output file: {e}")));
    }

    Ok(quad_count)
}

/// A Python module implemented in Rust.
#[pymodule]
fn rdformats(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert, m)?)?;
    Ok(())
}
