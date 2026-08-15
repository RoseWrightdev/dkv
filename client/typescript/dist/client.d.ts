/**
 * Client — a typed TypeScript wrapper around the ORYX gRPC service.
 *
 * Usage (insecure):
 *   import { insecureCredentials } from "./server";
 *   const client = Client.connect("localhost:50051", insecureCredentials());
 *
 * Usage (TLS):
 *   import { tlsCredentials } from "./server";
 *   const client = Client.connect("my-host:50051", tlsCredentials());
 */
import * as grpc from "@grpc/grpc-js";
export interface ClientOptions {
    /** Per-call timeout in milliseconds.  Defaults to 5 000 ms. */
    timeoutMs?: number;
    /** Additional gRPC channel options (e.g. keep-alive, max message size). */
    channelOptions?: Partial<grpc.ClientOptions>;
}
export interface GetResult {
    /** The raw bytes stored for the key, or null when the key does not exist. */
    value: Buffer | null;
    exists: boolean;
}
export declare class Client {
    private readonly stub;
    private readonly timeoutMs;
    private constructor();
    /**
     * Open a connection to a ORYX node.
     *
     * @param address      Host and port of the target ORYX node, e.g. `"localhost:50051"`.
     * @param credentials  Channel credentials — use `insecureCredentials()` for plaintext or `tlsCredentials()` for secure channels.
     * @param options      Optional configuration details including per-call timeoutMs and custom gRPC channelOptions.
     * @returns A connected `Client` instance.
     */
    static connect(address: string, credentials: grpc.ChannelCredentials, options?: ClientOptions): Client;
    /**
     * Retrieve the value associated with `key`.
     *
     * @param key The unique identifier string whose value to retrieve.
     * @returns A promise resolving to a `GetResult` containing `{ value: Buffer | null, exists: boolean }`.
     */
    get(key: string): Promise<GetResult>;
    /**
     * Store `value` under `key`.
     *
     * @param key   The unique identifier string under which to store the value.
     * @param value The raw data payload to store, passed as a Node.js `Buffer` or `Uint8Array`.
     * @returns A promise that resolves when the operation is successfully completed.
     */
    set(key: string, value: Buffer | Uint8Array): Promise<void>;
    /**
     * Remove `key` and its associated value from the store.
     *
     * @param key The unique identifier string to remove.
     * @returns A promise that resolves once deleted. Resolves even if the key did not exist in the store.
     */
    delete(key: string): Promise<void>;
    /**
     * Close the underlying gRPC channel.
     * The client instance must not be used for any further operations after this call.
     */
    close(): void;
    /** Returns a deadline `Date` based on the configured per-call timeout. */
    private deadline;
}
//# sourceMappingURL=client.d.ts.map