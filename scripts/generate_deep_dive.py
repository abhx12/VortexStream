# scripts/generate_deep_dive.py
"""Assembles all doc parts into PROJECT_DEEP_DIVE.md in the project root."""

from pathlib import Path
import sys

# Ensure scripts directory is on sys.path
scripts_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(scripts_dir))

from doc_parts.part1 import get_part1
from doc_parts.part2 import get_part2
from doc_parts.part3 import get_part3
from doc_parts.part4 import get_part4
from doc_parts.part5 import get_part5

def build_deep_dive():
    print("Assembling PROJECT_DEEP_DIVE.md...")
    p1 = get_part1().strip()
    p2 = get_part2().strip()
    p3 = get_part3().strip()
    p4 = get_part4().strip()
    p5 = get_part5().strip()

    full_document = "\n\n".join([p1, p2, p3, p4, p5]) + "\n"

    # Write to root directory of project
    root_dir = scripts_dir.parent
    target_file = root_dir / "PROJECT_DEEP_DIVE.md"

    with open(target_file, "w", encoding="utf-8") as f:
        f.write(full_document)

    line_count = len(full_document.splitlines())
    char_count = len(full_document)
    file_size_kb = target_file.stat().st_size / 1024

    print(f"SUCCESS: Generated {target_file}")
    print(f"Lines: {line_count:,}")
    print(f"Characters: {char_count:,}")
    print(f"Size: {file_size_kb:.2f} KB")

if __name__ == "__main__":
    build_deep_dive()
