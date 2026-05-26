import sys
from dkv import DKVClient, insecure_credentials


def main():
    print("Connecting to DKV server...")
    client = DKVClient.connect("127.0.0.1:50051", insecure_credentials())


    key = "python-example-key"
    val = b"Hello DKV from Python!"

    try:
        print("Storing value under key...")
        client.set(key, val)

        print("Retrieving value...")
        retrieved_val = client.get(key)
        if retrieved_val is not None:
            string_val = retrieved_val.decode("utf-8")
            print(f"Success! Retrieved value: \"{string_val}\"")
        else:
            print("Key not found!")

        print("Deleting key...")
        client.delete(key)

        print("Verifying deletion...")
        check = client.get(key)
        print(f"Key exists: {check is not None}")

        print("Done!")
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
    finally:
        client.close()

if __name__ == "__main__":
    main()
