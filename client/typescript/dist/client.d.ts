/**
 * DkvClient — a typed TypeScript wrapper around the DKV gRPC service.
 *
 * Usage (insecure):
 *   import { insecureCredentials } from "./server";
 *   const client = DkvClient.connect("localhost:50051", insecureCredentials());
 *
 * Usage (TLS):
 *   import { tlsCredentials } from "./server";
 *   const client = DkvClient.connect("my-host:50051", tlsCredentials());
 */
import * as grpc from "@grpc/grpc-js";
export interface DkvClientOptions {
    /** Per-call timeout in milliseconds.  Defaults to 5 000 ms. */
    timeoutMs?: number;
    /** Additional gRPC channel options (e.g. keep-alive, max message size). */
    channelOptions?: Partial<grpc.ClientOptions>;
}
export interface GetResult {
    /** The raw bytes stored for the key, or null when the key does not exist. */
    value: Buffer | null;
    /** True when the key was found in the store. */
    exists: boolean;
}
export declare class DkvClient {
    private readonly stub;
    private readonly timeoutMs;
    private constructor();
    /**
     * Open a connection to a DKV node.
     *
     * @param address      Host and port, e.g. `"localhost:50051"`.
     * @param credentials  Channel credentials — use `insecureCredentials()` or `tlsCredentials()`.
     * @param options      Optional timeout and channel settings.
     */
    static connect(address: string, credentials: grpc.ChannelCredentials, options?: DkvClientOptions): DkvClient;
    /**
     * Retrieve the value associated with `key`.
     *
     * @returns `{ value, exists }` — `value` is `null` when the key is absent.
     */
    get(key: string): Promise<GetResult>;
    /**
     * Store `value` under `key`.  Accepts a `Buffer` or `Uint8Array`.
     */
    set(key: string, value: Buffer | Uint8Array): Promise<void>;
    /**
     * Remove `key` from the store.  Resolves even when the key did not exist.
     */
    delete(key: string): Promise<void>;
    /**
     * Close the underlying gRPC channel.  The client must not be used after
     * this call.
     */
    close(): void;
    /** Returns a deadline `Date` based on the configured per-call timeout. */
    private deadline;
}
//# sourceMappingURL=client.d.ts.map