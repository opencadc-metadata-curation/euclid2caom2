import sys
import tempfile
import os
from cadcdata import StorageInventoryClient
from cadcutils.net import Subject

# parameters:
# 1 - file with list of work to be done
# 2 - certificate fully-qualified name

def main():
    if len(sys.argv) != 3:
        print("Usage: python delete_si_uris.py <work_list_file> <certificate>")
        sys.exit(1)

    work_list_file = sys.argv[1]
    certificate = sys.argv[2]

    subject = Subject(certificate=certificate)
    client = StorageInventoryClient(subject=subject)

    with open(work_list_file, 'r') as f:
        uris = [line.strip() for line in f if line.strip()]

    for old_uri in uris:
        client.cadcremove(old_uri)
        print(f'remove {old_uri}')

if __name__ == "__main__":
    main()

