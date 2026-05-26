/**
 * dkv-ts — TypeScript client for the DKV distributed key-value store.
 *
 * Public API:
 *   - DkvClient          — the main client class (connect / get / set / delete / close)
 *   - DkvClientOptions   — options accepted by DkvClient.connect()
 *   - GetResult          — return type of DkvClient.get()
 *   - insecureCredentials() — creates plain-text channel credentials
 *   - tlsCredentials()      — creates TLS channel credentials
 */
export { DkvClient } from "./client";
export type { DkvClientOptions, GetResult } from "./client";
export { insecureCredentials, tlsCredentials } from "./server";
//# sourceMappingURL=index.d.ts.map