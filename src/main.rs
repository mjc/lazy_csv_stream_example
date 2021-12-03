use std::{
    env,
    error::Error,
    ffi::OsString,
    fs::{File, OpenOptions},
};

use csv::{QuoteStyle, Reader, Trim, Writer, WriterBuilder};

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
    let headers = reader.headers().expect("Could not get headers");
    dbg!(headers);
    let second_header = headers
        .into_iter()
        .nth(1)
        .expect("could not get second header");
    let fourth_header = headers
        .into_iter()
        .nth(3)
        .expect("could not get fourth header");

    writer
        .write_record([second_header, fourth_header])
        .expect("could not write headers");

    for result in reader.records() {
        let record = result.expect("got malformed sv");
        let second = record
            .into_iter()
            .nth(1)
            .expect("could not find second column");
        let fourth = record
            .into_iter()
            .nth(3)
            .expect("could not find fourth column");
        dbg!(second, fourth);

        writer
            .write_record([second, fourth])
            .expect("could not write record");
    }

    Ok(())
}

fn get_file_name_from_first_argument() -> OsString {
    match env::args_os().nth(1) {
        None => From::from("expected 1 argument, but got none"),
        Some(file_path) => file_path,
    }
}

fn main() {
    let provided_filename = get_file_name_from_first_argument()
        .to_str()
        .expect("file name problem")
        .to_string();
    let out_file = "output.csv".to_string();
    match stream_read_and_write(provided_filename, out_file) {
        Ok(_) => println!("Written successfully"),
        Err(e) => println!("Error: {:?}", e),
    }
}
