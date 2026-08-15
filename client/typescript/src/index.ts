/**
 * oryx-ts — TypeScript client for the ORYX distributed key-value store.
 *
 * Public API:
 *   - OryxClient          — the main client class (connect / get / set / delete / close)
 *   - OryxClientOptions   — options accepted by OryxClient.connect()
 *   - GetResult          — return type of OryxClient.get()
 *   - insecureCredentials() — creates plain-text channel credentials
 *   - tlsCredentials()      — creates TLS channel credentials
 */

export { Client } from "./client";
export type { ClientOptions, GetResult } from "./client";
export { insecureCredentials, tlsCredentials } from "./server";
