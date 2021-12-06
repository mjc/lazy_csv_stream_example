use std::{
    env,
    fs::{File, OpenOptions},
    time::Duration,
};

use pbr::ProgressBar;
use serde::{Deserialize, Serialize};

use csv::{QuoteStyle, Reader, Trim, Writer, WriterBuilder};

#[derive(Debug, Serialize, Deserialize)]
struct MyRow {
    time: String,
    butts: String,
}

fn csv_reader_stream(in_file_path: String) -> Result<Reader<File>, std::io::Error> {
    let input_file = File::open(in_file_path)?;
    let reader = csv::ReaderBuilder::new()
        .trim(Trim::Headers)
        .from_reader(input_file);
    Ok(reader)
}

fn csv_writer_stream(out_file_path: String) -> Result<Writer<File>, std::io::Error> {
    // File.open does not create new files by default
    // so we must use OpenOptions manually instead.
    let output_file = open_output_file(out_file_path)?;
    let writer = WriterBuilder::new()
        .quote_style(QuoteStyle::Never)
        .from_writer(output_file);
    Ok(writer)
}

fn open_output_file(out_file_path: String) -> Result<File, std::io::Error> {
    /*
        In nightly you can do

        File::with_options()
            .create(true)
            .write(true)
            .open(out_file_path)
    */
    OpenOptions::new()
        .write(true)
        .create(true)
        .open(out_file_path)
}

fn stream_read_and_write(
    input_filename: String,
    output_filename: String,
) -> Result<(), std::io::Error> {
    let mut reader = csv_reader_stream(input_filename)?;
    let mut writer = csv_writer_stream(output_filename)?;

    // approximate line width
    // if csv reader would give us the file position we would not need this
    let line_width = 30;
    let file_size = reader.get_ref().metadata()?.len();
    let total_lines_estimate = file_size / (line_width as u64);

    let mut progress = ProgressBar::new(total_lines_estimate);
    // massive files so let's not update so damn fast
    progress.set_max_refresh_rate(Some(Duration::from_secs(1)));

    for result in reader.deserialize() {
        let row: MyRow = result?;
        // update these  and struct fields if column names are updated
        // using serde and your own struct means it automatically adds headers
        writer.serialize(row)?;

        progress.inc();
    }
    progress.finish();
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
