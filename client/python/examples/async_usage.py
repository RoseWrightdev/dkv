import asyncio
import sys

from dkv import DKVAsyncClient, insecure_credentials


async def main():
    print("Connecting to DKV server asynchronously...")
    client = DKVAsyncClient.connect("127.0.0.1:50051", insecure_credentials())

    key = "python-async-example-key"
    val = b"Hello async Python!"

    try:
        print("Storing value under key asynchronously...")
        await client.set(key, val)

        print("Retrieving value...")
        retrieved_val = await client.get(key)
        if retrieved_val is not None:
            string_val = retrieved_val.decode("utf-8")
            print(f'Success! Retrieved value: "{string_val}"')
        else:
            print("Key not found!")

        print("Deleting key...")
        await client.delete(key)

        print("Verifying deletion...")
        check = await client.get(key)
        print(f"Key exists: {check is not None}")

        print("Done!")
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
    finally:
        # Close the async channel properly
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
