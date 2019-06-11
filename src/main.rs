use flate2;
use std::io::Write;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use hashbrown::HashMap;
use std::collections::BinaryHeap;
use nom::IResult;
use nom::{named, tag, is_not, tuple, is_a, take_till, take_while, is_digit, take_str, alt, dbg_dmp};

const LIST_TOP: usize = 10;
const PACKAGE_TOP: usize = 100;

#[derive(structopt::StructOpt)]
struct Args {
    /// Directory containing all gzips
    #[structopt(name = "PATH", parse(from_os_str))]
    path: std::path::PathBuf,

    /// Path to save all parsed lists
    #[structopt(short = "l", long = "list", parse(from_os_str))]
    list: Option<std::path::PathBuf>,

    /// Path to save all parsed packages 
    #[structopt(short = "p", long = "package", parse(from_os_str))]
    package: Option<std::path::PathBuf>,
}

#[derive(Hash, PartialEq, Eq, Debug, Clone, PartialOrd, Ord)]
struct Package {
    seg_1: String,
    seg_2: String,
    seg_3: String,
    package: String,
}

#[derive(Hash, PartialEq, Eq, Debug, Clone, PartialOrd, Ord)]
struct List {
    name: String,
}

#[derive(Hash, PartialEq, Eq, Debug, Clone, PartialOrd, Ord)]
enum Content {
    List(List),
    Package(Package),
    ListAll,
}

#[derive(Debug)]
enum Line {
    NotModified(Content),
    Ok(Content, usize),
}

named!(skip_head, is_not!("\""));
named!(match_get, tag!("\"GET "));

fn parse_list(input: &[u8]) -> IResult<&[u8], Content> {
    named!(parse<&[u8], (&[u8], &[u8], &[u8])>,
        tuple!(
            tag!("/simple/"),
            take_till!(|ch| ch == b'/'),
            alt!(tag!("/ HTTP/1.1\" ") | tag!("/ HTTP/2.0\" "))
        )
    );

    let (input, (_, name, _)) = parse(input)?;
    return Ok((input, Content::List(List { name: std::str::from_utf8(name).unwrap().to_owned() })));
}

fn parse_list_all(input: &[u8]) -> IResult<&[u8], Content> {
    named!(parse<&[u8], (&[u8], &[u8])>,
        tuple!(
            tag!("/simple/"),
            alt!(tag!(" HTTP/1.1\" ") | tag!(" HTTP/2.0\" "))
        )
    );

    let (input, _) = parse(input)?;
    return Ok((input, Content::ListAll));
}

fn parse_package(input: &[u8]) -> IResult<&[u8], Content> {
    named!(parse<&[u8], (&[u8], &str, &[u8], &str, &[u8], &str, &[u8], &[u8], &[u8])>,
        tuple!(
            tag!("/packages/"),
            take_str!(2),
            is_a!("/"),
            take_str!(2),
            is_a!("/"),
            take_str!(60),
            is_a!("/"),
            take_till!(|ch| ch == b' '),
            alt!(tag!(" HTTP/1.1\" ") | tag!(" HTTP/2.0\" "))
        )
    );

    let (input, (_, seg_1, _, seg_2, _, seg_3, _, package, _)) = parse(input)?;

    let package = Package {
        seg_1: seg_1.to_owned(),
        seg_2: seg_2.to_owned(),
        seg_3: seg_3.to_owned(),
        package: std::str::from_utf8(package).unwrap().to_owned(),
    };

    Ok((input, Content::Package(package)))
}

named!(parse_url(&[u8]) -> Content, alt!(parse_package | parse_list | parse_list_all));
named!(parse_resp, alt!(tag!("304") | tag!("200")));
named!(parse_size<&[u8], (&[u8], &[u8])>, tuple!(is_a!(" "), take_while!(is_digit)));

fn parse_line(input: &[u8]) -> IResult<&[u8], Line> {
    // Parse header
    let (input, _) = skip_head(input)?;
    let (input, _) = match_get(input)?;

    let (input, content) = parse_url(input)?;
    let (input, resp) = parse_resp(input)?;

    if std::str::from_utf8(resp).unwrap() == "304" {
        return Ok((input, Line::NotModified(content)));
    } else {
        let (input, (_, size)) = parse_size(input)?;
        return Ok((input, Line::Ok(content, std::str::from_utf8(size).unwrap().parse::<usize>().unwrap())));
    }
}

#[paw::main]
fn main(args: Args) -> Result<(), std::io::Error> {
    let mut counts: HashMap<Content, usize> = HashMap::new();
    let mut sizes: HashMap<Package, usize> = HashMap::new();

    for entry in std::fs::read_dir(args.path)? {
        let entry = entry?;
        println!("Processing {}...", entry.path().display());
        let file = std::fs::File::open(entry.path())?;
        let d = flate2::read::GzDecoder::new(file);
        let d = BufReader::new(d);

        let mut count = 0;
        let mut valid = 0;

        for line in d.lines() {
            count += 1;
            let line = match parse_line(line?.as_bytes()) {
                Err(_) => continue,
                Ok((_, line)) => line,
            };

            valid += 1;
            if valid % 10000 == 0 {
                println!("Progress(valid/total): {}/{}", valid, count);
            }

            match line {
                Line::Ok(content, size) => {
                    *counts.entry(content.clone()).or_insert(0) += 1;

                    if let Content::Package(package) = content {
                        sizes.entry(package).or_insert(size);
                    }
                },
                Line::NotModified(content) => {
                    *counts.entry(content).or_insert(0) += 1;
                }
            }
        }

        println!("Finish. \n  Total lines: {}, Valid lines: {}", count, valid);
    }

    println!("Stats: ");
    println!("List alls: {}", counts.get(&Content::ListAll).unwrap_or(&0));

    let mut list_heap: BinaryHeap<(usize, List)> = BinaryHeap::new();
    let mut package_heap: BinaryHeap<(usize, Package)> = BinaryHeap::new();

    for (k, v) in counts.into_iter() {
        match k {
            Content::ListAll => continue,
            Content::List(list) => list_heap.push((v, list)),
            Content::Package(package) => package_heap.push((v, package)),
        }
    }

    let mut list_writer = args.list.map(std::fs::File::create).map(Result::unwrap).map(BufWriter::new);
    let mut package_writer = args.package.map(std::fs::File::create).map(Result::unwrap).map(BufWriter::new);

    let mut counter = 0;
    let mut tot_count = 0;
    println!("Total parsed lists: {}", list_heap.len());
    println!("Top {} listed directories: ", LIST_TOP);
    if let Some(ref mut w) = list_writer {
        writeln!(w, "{}", list_heap.len())?;
    }
    while let Some((count, list)) = list_heap.pop() {
        if counter < LIST_TOP {
            println!("  {: <20}: {}", list.name, count);
        }
        counter += 1;
        if let Some(ref mut w) = list_writer {
            writeln!(w, "{} {}", list.name, count)?;
        }

        tot_count += count;
    }
    println!("Transfer count: {}", tot_count);
    println!();

    let mut tot_transfer: f64= 0f64;
    let mut tot_count: f64 = 0f64;

    let mut counter = 0;
    println!("Total parsed packages: {}", package_heap.len());
    println!("Top {} requested packages: ", PACKAGE_TOP);
    if let Some(ref mut w) = package_writer {
        writeln!(w, "{}", package_heap.len())?;
    }
    while let Some((count, package)) = package_heap.pop() {
        let raw_size = sizes.get(&package);
        if counter <= PACKAGE_TOP {
            let size = raw_size.map(|e| pretty_bytes::converter::convert(*e as f64)).unwrap_or_else(|| "Unknown size".to_owned());
            println!("  {: <60} {: >8}, {}", package.package, count, size);
        }
        counter += 1;
        if let Some(ref mut w) = package_writer {
            writeln!(w, "{} {} {} {} {} {}", package.seg_1, package.seg_2, package.seg_3, package.package, count, raw_size.unwrap_or(&0))?;
        }

        tot_transfer += count as f64 * (*raw_size.unwrap_or(&0)) as f64;
        tot_count += count as f64;
    }

    let avg = tot_transfer / tot_count;
    println!("Transfer count: {}", tot_count);
    println!("Average transfer size: {}", pretty_bytes::converter::convert(avg));

    Ok(())
}
