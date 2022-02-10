from pathlib import Path
import sys


def main():
    # Passthrough test for now.
    mcap_path = sys.argv[1].replace(".json", ".mcap")
    data = Path(mcap_path).read_bytes()
    hex = "".join("{:02x}".format(x) for x in data)
    print(hex)


if __name__ == "__main__":
    main()
