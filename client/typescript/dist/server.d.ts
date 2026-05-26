/**
 * server.ts — convenience re-export of the gRPC server utilities.
 *
 * This file is a thin shim so that consumers who only need to talk to a
 * DKV cluster don't have to import from @grpc/grpc-js directly for the
 * common credential variants.
 */
import * as grpc from "@grpc/grpc-js";
/**
 * Creates insecure (plain-text) channel credentials.
 * Mirrors `insecure.NewCredentials()` from the Go client.
 */
export declare function insecureCredentials(): grpc.ChannelCredentials;
/**
 * Creates TLS channel credentials using the system CA bundle.
 * Pass PEM buffers for mutual TLS or custom CA configurations.
 *
 * @param rootCerts  PEM-encoded root certificates, or omit for system CAs.
 * @param privateKey PEM-encoded private key for mTLS.
 * @param certChain  PEM-encoded certificate chain for mTLS.
 */
export declare function tlsCredentials(rootCerts?: Buffer, privateKey?: Buffer, certChain?: Buffer): grpc.ChannelCredentials;
//# sourceMappingURL=server.d.ts.map