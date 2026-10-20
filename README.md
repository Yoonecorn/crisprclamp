# CrisprClamp

## What CrisprClamp does

CrisprClamp counts guide sequences and expected guide pairs from FASTQ input,
including paired-end dual-guide screens. It reports raw counts and read classes.

## What it does not do

It does not perform downstream statistical inference, hit calling, or biological
interpretation. This repository provides the counting CLI and no graphical app.

## Requirements

Apple Silicon macOS is the only tested platform. Install a Rust toolchain with
Rust 2024 edition support and the macOS command line developer tools. Python 3
is required for the example table export.

Tested with Rust 1.98.0.

## Build from source

Run from the repository root:

```sh
cargo build --release --locked
```

Shortened build output:

```text
Finished `release` profile [optimized] target(s)
```

## Quick start

Use a new output directory for each run:

```sh
./target/release/crisprclamp --mode dual --sample sample_tiny \
  --fastq examples/tiny_dataset/R1.fastq.gz \
  --fastq-r2 examples/tiny_dataset/R2.fastq.gz \
  --guides-a examples/tiny_dataset/guides_a.tsv \
  --guides-b examples/tiny_dataset/guides_b.tsv \
  --constructs examples/tiny_dataset/constructs.tsv \
  --a-start 0 --b-start 0 --b-source R2 \
  --memory-budget-bytes 2147483648 --output ./tiny_output
python3 examples/tiny_dataset/export_table.py \
  --constructs examples/tiny_dataset/constructs.tsv \
  --jsonl tiny_output/raw-counts.jsonl --output counts.tsv
cat counts.tsv
```

The counting command prints resource and timing diagnostics. The export and
print commands produce:

```text
Exported 5 rows to counts.tsv
construct_id	guide_a	guide_b	count
C01	G01	H01	2
C02	G02	H02	1
C03	G03	H03	0
C04	G04	H04	0
C05	G05	H05	0
```

## License

Copyright 2026 Yoon Sing Yap. Licensed under MIT OR Apache-2.0, at your option.
See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).

## Contact

[GitHub Issues](https://github.com/Yoonecorn/crisprclamp/issues)

The manuscript link will be added.
