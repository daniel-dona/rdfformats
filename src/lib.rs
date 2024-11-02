use std::io::{BufReader, BufWriter, Write};

use pyo3::exceptions::{PyRuntimeError, PyIOError};
use pyo3::prelude::*;


use oxrdfio::{RdfFormat, RdfParser, RdfSerializer};

/// Formats the sum of two numbers as string.
#[pyfunction]
fn convert(from_file: String, to_file: String, from_format: String, to_format: String) -> PyResult<u64> {

    let from_format_value = match from_format.as_str() {
        "ttl" => Ok(RdfFormat::Turtle),
        "nt" => Ok(RdfFormat::NTriples),
        "nq" => Ok(RdfFormat::NQuads),
        "n3" => Ok(RdfFormat::N3),
        "trig" => Ok(RdfFormat::TriG),
        "rdf/xml" => Ok(RdfFormat::RdfXml), 
        _ => Err(PyIOError::new_err(format!("Wrong value for format: {}", from_format))),
    }.unwrap();

    let to_format_value = match to_format.as_str() {
        "ttl" => Ok(RdfFormat::Turtle),
        "nt" => Ok(RdfFormat::NTriples),
        "nq" => Ok(RdfFormat::NQuads),
        "n3" => Ok(RdfFormat::N3),
        "trig" => Ok(RdfFormat::TriG),
        "rdf/xml" => Ok(RdfFormat::RdfXml), 
        _ => Err(PyIOError::new_err(format!("Wrong value for format: {}", from_format))),
    }.unwrap();

    // Open input and output files
    let from_file = match std::fs::File::open(from_file.clone()) {
        Ok(file) => file,
        Err(e) => return Err(PyIOError::new_err(format!("Failed to open {}: {}", from_file, e))),
    };
    
    let to_file = match std::fs::File::create(to_file.clone()) {
        Ok(file) => file,
        Err(e) => return Err(PyIOError::new_err(format!("Failed to create {}: {}", to_file, e))),
    };

    // Create a buffered reader for the input file
    let mut reader = BufReader::with_capacity(1024*1024,from_file);
    
    // Create a buffered writer for the output file
    let mut writer = BufWriter::with_capacity(1024*1024, to_file);

    // Streaming parser setup (assuming RdfParser supports streaming)
    let mut parser = RdfParser::from_format(from_format_value).for_reader(&mut reader);

    // Streaming serializer setup
    let mut serializer = RdfSerializer::from_format(to_format_value).for_writer(&mut writer);

    // Count triples/quads
    let mut my_int: u64 = 0;

    // Process the file streamingly
    while let Some(result) = parser.next() {
        match result {
            Ok(quad) => {
                if let Err(e) = serializer.serialize_quad(&quad) {
                    return Err(PyRuntimeError::new_err(format!("Serialization error: {}", e)));
                }else{
                    my_int += 1;
                }
            },
            Err(e) => {
                return Err(PyRuntimeError::new_err(format!("Parsing error: {}", e)));
            }
        }
    }

    if let Err(e) = serializer.finish() {
        return Err(PyIOError::new_err(format!("Failed to flush serializer: {}", e)));
    }

    // Ensure the writer is flushed (if necessary, depending on the library's behavior)
    if let Err(e) = writer.flush() {
        return Err(PyIOError::new_err(format!("Failed to flush output file: {}", e)));
    }

    Ok(my_int)
}

/// A Python module implemented in Rust.
#[pymodule]
fn rdformats(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert, m)?)?;
    Ok(())
}
