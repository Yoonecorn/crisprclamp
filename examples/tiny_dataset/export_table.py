#!/usr/bin/env python3
"""
export_table.py: Turn CrisprClamp count output into a TSV table.

Usage:
  # For default JSONL output:
  python3 export_table.py --constructs constructs.tsv --jsonl raw-counts.jsonl --output counts.tsv

  # For compact binary counts:
  python3 export_table.py --constructs constructs.tsv --counts counts.u64le --output counts.tsv
  or
  python3 export_table.py --descriptor expected-pairs.json --guides-a guides_a.tsv --guides-b guides_b.tsv --counts counts.u64le --output counts.tsv
"""

import argparse
import json
from pathlib import Path
import struct
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="Convert CrisprClamp count output to a TSV table.")
    parser.add_argument("--constructs", help="Path to constructs.tsv (construct_id, guide_a, guide_b)")
    parser.add_argument("--descriptor", help="Path to expected-pairs.json descriptor")
    parser.add_argument("--guides-a", help="Path to guides_a.tsv")
    parser.add_argument("--guides-b", help="Path to guides_b.tsv")
    parser.add_argument("--counts", help="Path to binary count vector (counts.u64le)")
    parser.add_argument("--jsonl", help="Path to JSONL count file (raw-counts.jsonl)")
    parser.add_argument("--output", required=True, help="Path to output TSV table")
    return parser.parse_args()

def main():
    args = parse_args()
    out_path = Path(args.output)

    if args.jsonl:
        if not args.constructs:
            sys.exit("Error: --constructs is required when using --jsonl")
        jsonl_path = Path(args.jsonl)
        if not jsonl_path.exists():
            sys.exit(f"Error: jsonl file not found: {jsonl_path}")
        constructs_path = Path(args.constructs)
        if not constructs_path.exists():
            sys.exit(f"Error: constructs file not found: {constructs_path}")

        counts_map = {}
        with jsonl_path.open("r", encoding="utf-8") as fj:
            for line in fj:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                cid = row.get("row_id")
                c_val = row.get("counts", [0])[0]
                counts_map[cid] = c_val

        total_rows = 0
        with constructs_path.open("r", encoding="utf-8") as fc, out_path.open("w", encoding="utf-8") as fo:
            fo.write("construct_id\tguide_a\tguide_b\tcount\n")
            for line in fc:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) < 3:
                    sys.exit(f"Error: invalid constructs.tsv line: {line}")
                cid, ga, gb = parts[0], parts[1], parts[2]
                cnt = counts_map.get(cid, 0)
                fo.write(f"{cid}\t{ga}\t{gb}\t{cnt}\n")
                total_rows += 1
        print(f"Exported {total_rows} rows to {out_path}")
        return

    if not args.counts:
        sys.exit("Error: either --jsonl or --counts must be provided.")

    counts_path = Path(args.counts)
    if not counts_path.exists():
        sys.exit(f"Error: counts file not found: {counts_path}")
    
    raw_counts = counts_path.read_bytes()
    if len(raw_counts) % 8 != 0:
        sys.exit(f"Error: invalid count file size: {len(raw_counts)} bytes (must be a multiple of 8)")
    total_rows = len(raw_counts) // 8
    counts = struct.unpack(f"<{total_rows}Q", raw_counts)

    if args.constructs:
        constructs_path = Path(args.constructs)
        if not constructs_path.exists():
            sys.exit(f"Error: constructs file not found: {constructs_path}")
        with constructs_path.open("r", encoding="utf-8") as fc, out_path.open("w", encoding="utf-8") as fo:
            fo.write("construct_id\tguide_a\tguide_b\tcount\n")
            row_idx = 0
            for line in fc:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) < 3:
                    sys.exit(f"Error: invalid constructs.tsv line at row {row_idx}: {line}")
                if row_idx >= total_rows:
                    sys.exit(f"Error: more constructs than count rows ({total_rows})")
                fo.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\t{counts[row_idx]}\n")
                row_idx += 1
            if row_idx != total_rows:
                sys.exit(f"Error: constructs count ({row_idx}) does not match count rows ({total_rows})")

    elif args.descriptor:
        if not args.guides_a or not args.guides_b:
            sys.exit("Error: --guides-a and --guides-b are required when using --descriptor")
        
        with open(args.guides_a, "r", encoding="utf-8") as f:
            guides_a = [l.strip().split("\t")[0] for l in f if l.strip()]
        with open(args.guides_b, "r", encoding="utf-8") as f:
            guides_b = [l.strip().split("\t")[0] for l in f if l.strip()]

        desc_path = Path(args.descriptor)
        desc = json.loads(desc_path.read_text(encoding="utf-8"))
        ref_loc = Path(desc["reference"])
        if not ref_loc.is_absolute():
            ref_loc = desc_path.parent / ref_loc
        
        raw_ref = ref_loc.read_bytes()
        pair_count = desc["pair_count"]
        if len(raw_ref) != pair_count * 8:
            sys.exit(f"Error: reference size mismatch: expected {pair_count * 8} bytes, got {len(raw_ref)}")
        if total_rows != pair_count:
            sys.exit(f"Error: counts rows ({total_rows}) does not match descriptor pair_count ({pair_count})")
        
        pairs = struct.unpack(f"<{pair_count * 2}I", raw_ref)
        first_ord = desc.get("first_pair_ordinal", 0)

        with out_path.open("w", encoding="utf-8") as fo:
            fo.write("ordinal\tconstruct_id\tguide_a\tguide_b\tcount\n")
            for i in range(pair_count):
                ai = pairs[2 * i]
                bi = pairs[2 * i + 1]
                ga = guides_a[ai] if ai < len(guides_a) else f"index_{ai}"
                gb = guides_b[bi] if bi < len(guides_b) else f"index_{bi}"
                cid = f"construct_{first_ord + i:07d}"
                fo.write(f"{first_ord + i}\t{cid}\t{ga}\t{gb}\t{counts[i]}\n")
    else:
        sys.exit("Error: either --constructs or --descriptor must be provided.")

    print(f"Exported {total_rows} rows to {out_path}")

if __name__ == "__main__":
    main()
