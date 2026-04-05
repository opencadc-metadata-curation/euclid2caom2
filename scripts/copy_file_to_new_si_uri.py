import sys
import tempfile
import os
from cadcdata import StorageInventoryClient
from cadcutils.net import Subject

# parameters:
# 1 - collection name
# 2 - file with list of work to be done
# 3 - old uri pattern
# 4 - new uri pattern
# 5 - certificate fully-qualified name

# use the CadcDataClient and the 5th parameter to create a client instance
# for each uri in the list of work to be done
# - use the get method to read the file to a temporary staging location
# - rename the uri according to the new uri pattern
# - use the put method to write the file with the new uri pattern from the temporary staging location
def main():
    if len(sys.argv) != 5:
        print("Usage: python copy_file_to_new_si_uri.py <work_list_file> <old_uri_pattern> <new_uri_pattern> <certificate>")
        sys.exit(1)

    work_list_file = sys.argv[1]
    old_uri_pattern = sys.argv[2]
    new_uri_pattern = sys.argv[3]
    certificate = sys.argv[4]

    subject = Subject(certificate=certificate)
    client = StorageInventoryClient(subject=subject)

    with open(work_list_file, 'r') as f:
        uris = [line.strip() for line in f if line.strip()]

    for old_uri in uris:
        # Download the file to a temporary location
        # with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        #     tmp_path = tmp_file.name
        tmp_path = f'./{os.path.basename(old_uri)}'
        try:
            print(f'get {old_uri} to {tmp_path}')
            client.cadcget(old_uri, tmp_path)

            # Generate new URI
            new_uri = old_uri.replace(old_uri_pattern, new_uri_pattern)

            # Upload the file to the new URI
            client.cadcput(new_uri, tmp_path)
            print(f"Copied {old_uri} to {new_uri}")
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

if __name__ == "__main__":
    main()

