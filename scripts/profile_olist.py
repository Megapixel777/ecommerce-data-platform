import csv
from pathlib import Path


DATA_DIR = Path(r"C:\Users\thoma\OneDrive\Desktop\olist")


def profile_csv(path: Path) -> None:
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.reader(file)

        header = next(reader)
        row_count = sum(1 for _ in reader)

    print("=" * 80)
    print(path.name)
    print("=" * 80)
    print(f"Rows:    {row_count:,}")
    print(f"Columns: {len(header)}")
    print("Schema:")

    for column in header:
        print(f"  - {column}")


def main() -> None:
    for path in sorted(DATA_DIR.glob("*.csv")):
        profile_csv(path)


if __name__ == "__main__":
    main()