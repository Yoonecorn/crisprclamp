# CrisprClamp Tiny Synthetic Dataset

This directory contains a verified synthetic dataset for testing CrisprClamp.

## Files

1. `guides_a.tsv`: Guide A definitions (5 guides, 20 nt each, tab-separated).
2. `guides_b.tsv`: Guide B definitions (5 guides, 20 nt each, tab-separated).
3. `constructs.tsv`: Target construct definitions (5 pairs: C01 to C05, tab-separated).
4. `reference.bin`: Binary construct representation (5 pairs of 32-bit unsigned integers, 40 bytes total).
5. `expected-pairs.json`: Compact reference descriptor for CrisprClamp.
6. `R1.fastq.gz`: Paired-end Read 1 (9 reads, gzip compressed).
7. `R2.fastq.gz`: Paired-end Read 2 (9 reads, gzip compressed).
8. `export_table.py`: Helper script to convert JSONL counts or binary count vectors (`counts.u64le`) into a TSV table.

## Input Read Summary

The 9 paired reads demonstrate all five CrisprClamp read classes:

| Read ID | Guide A | Guide B | Expected Class | Description |
|---|---|---|---|---|
| read01_mapped_c01 | G01 | H01 | Mapped | Matches construct C01 |
| read02_mapped_c01 | G01 | H01 | Mapped | Matches construct C01 |
| read03_mapped_c02 | G02 | H02 | Mapped | Matches construct C02 |
| read04_recombinant | G01 | H02 | Recombinant | Both guides known, but pair not in reference |
| read05_unmapped_a_known | G01 | Unknown | Unmapped | Guide A known, Guide B unknown |
| read06_unmapped_neither | Unknown | Unknown | Unmapped | Neither guide matches library |
| read07_invalid_n | G01 (with 'N') | H01 | Invalid | Contains ambiguous 'N' character |
| read08_invalid_short | G01 (15 nt) | H01 | Invalid | Read is shorter than 20 nt window |
| read09_invalid_alpha | G01 | H01 (with 'X') | Invalid | Contains non-standard character 'X' |

## Expected Results

- Total records: 9
- Mapped: 3 (C01: 2, C02: 1, C03: 0, C04: 0, C05: 0)
- Recombinant: 1
- Ambiguous: 0
- Unmapped: 2
- Invalid: 3
- Input read conservation: 3 + 1 + 0 + 2 + 3 = 9
- Zero-count constructs: 3 (C03, C04, C05)
