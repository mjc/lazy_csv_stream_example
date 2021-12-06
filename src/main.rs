use std::{
    env,
    error::Error,
    fs::{File, OpenOptions},
};

use serde::{Deserialize, Serialize};

use csv::{QuoteStyle, Reader, Trim, Writer, WriterBuilder};

#[derive(Debug, Serialize, Deserialize)]
struct MyRow {
    time: String,
    butts: String,
}

// cargo run --release large.csv  53.87s user 3.94s system 85% cpu 1:07.54 total

fn csv_reader_stream(in_file_path: String) -> Result<Reader<File>, Box<dyn Error>> {
    let input_file = File::open(in_file_path)?;
    let reader = csv::ReaderBuilder::new()
        // this is the default
        .has_headers(true)
        // also the default
        .quoting(true)
        // remove whitespace from beginning and end of headers
        .trim(Trim::Headers)
        .from_reader(input_file);
    Ok(reader)
}

fn csv_writer_stream(out_file_path: String) -> Result<Writer<File>, Box<dyn Error>> {
    // File.open does not create new files by default
    // so we must use OpenOptions manually instead.
    let output_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(out_file_path)?;
    let writer = WriterBuilder::new()
        .quote_style(QuoteStyle::Never)
        .has_headers(true)
        .from_writer(output_file);
    Ok(writer)
}

fn stream_read_and_write(
    input_filename: String,
    output_filename: String,
) -> Result<(), Box<dyn Error>> {
    let mut reader = csv_reader_stream(input_filename)?;
    let mut writer = csv_writer_stream(output_filename)?;

    for result in reader.deserialize() {
        let row: MyRow = result?;
        // update these  and struct fields if column names are updated
        // using serde and your own struct means it automatically adds headers
        writer.serialize(row)?;
    }
    Ok(())
}

fn get_file_name_from_first_argument() -> String {
    env::args_os()
        .nth(1)
        .expect("No argument given")
        .to_str()
        .expect("Unicode path parsing problem")
        .to_string()
}

fn main() {
    let provided_filename = get_file_name_from_first_argument();
    let out_file = "output.csv".to_string();
    match stream_read_and_write(provided_filename, out_file) {
        Ok(_) => println!("Written successfully"),
        Err(e) => println!("Error: {:?}", e),
    }
}
