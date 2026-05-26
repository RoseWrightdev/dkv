"use strict";
/**
 * server.ts — convenience re-export of the gRPC server utilities.
 *
 * This file is a thin shim so that consumers who only need to talk to a
 * DKV cluster don't have to import from @grpc/grpc-js directly for the
 * common credential variants.
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.insecureCredentials = insecureCredentials;
exports.tlsCredentials = tlsCredentials;
const grpc = __importStar(require("@grpc/grpc-js"));
/**
 * Creates insecure (plain-text) channel credentials.
 * Mirrors `insecure.NewCredentials()` from the Go client.
 */
function insecureCredentials() {
    return grpc.credentials.createInsecure();
}
/**
 * Creates TLS channel credentials using the system CA bundle.
 * Pass PEM buffers for mutual TLS or custom CA configurations.
 *
 * @param rootCerts  PEM-encoded root certificates, or omit for system CAs.
 * @param privateKey PEM-encoded private key for mTLS.
 * @param certChain  PEM-encoded certificate chain for mTLS.
 */
function tlsCredentials(rootCerts, privateKey, certChain) {
    return grpc.credentials.createSsl(rootCerts, privateKey, certChain);
}
//# sourceMappingURL=server.js.map