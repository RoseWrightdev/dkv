import { Client } from "../src/client";
import { insecureCredentials } from "../src/server";

async function main() {
  // Connect to a local dkv server instance using insecure credentials
  const client = Client.connect("localhost:50051", insecureCredentials());

  const key = "example-greeting";
  const value = Buffer.from("Hello dkv from TypeScript!");

  try {
    console.log(`Storing value under key "${key}"...`);
    await client.set(key, value);

    console.log(`Retrieving value for key "${key}"...`);
    const result = await client.get(key);

    if (result.exists && result.value) {
      console.log(`Success! Value: "${result.value.toString("utf-8")}"`);
    } else {
      console.log("Key does not exist.");
    }

    console.log(`Deleting key "${key}"...`);
    await client.delete(key);

    const checkAgain = await client.get(key);
    console.log(`Verified deleted: ${!checkAgain.exists}`);
  } catch (error) {
    console.error("An error occurred during DKV operations:", error);
  } finally {
    // Always close the client to release underlying gRPC channel resources
    client.close();
  }
}

main().catch((err) => {
  console.error("Error running main:", err);
  process.exit(1);
});
