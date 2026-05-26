"use strict";
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
exports.DkvClient = void 0;
const grpc = __importStar(require("@grpc/grpc-js"));
const dkv_1 = require("./gen/api/dkv");
class DkvClient {
    constructor(stub, timeoutMs) {
        this.stub = stub;
        this.timeoutMs = timeoutMs;
    }
    /**
     * Open a connection to a DKV node.
     *
     * @param address      Host and port, e.g. `"localhost:50051"`.
     * @param credentials  Channel credentials — use `insecureCredentials()` or `tlsCredentials()`.
     * @param options      Optional timeout and channel settings.
     */
    static connect(address, credentials, options = {}) {
        const { timeoutMs = 5000, channelOptions = {} } = options;
        const stub = new dkv_1.DkvServiceClient(address, credentials, channelOptions);
        return new DkvClient(stub, timeoutMs);
    }
    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------
    /**
     * Retrieve the value associated with `key`.
     *
     * @returns `{ value, exists }` — `value` is `null` when the key is absent.
     */
    get(key) {
        return new Promise((resolve, reject) => {
            const meta = new grpc.Metadata();
            this.stub.get({ key }, meta, { deadline: this.deadline() }, (err, response) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(response.exists
                    ? { value: response.value, exists: true }
                    : { value: null, exists: false });
            });
        });
    }
    /**
     * Store `value` under `key`.  Accepts a `Buffer` or `Uint8Array`.
     */
    set(key, value) {
        return new Promise((resolve, reject) => {
            const meta = new grpc.Metadata();
            this.stub.set({ key, value: Buffer.from(value), timestamp: 0, nodeId: "" }, meta, { deadline: this.deadline() }, (err) => { if (err)
                reject(err);
            else
                resolve(); });
        });
    }
    /**
     * Remove `key` from the store.  Resolves even when the key did not exist.
     */
    delete(key) {
        return new Promise((resolve, reject) => {
            const meta = new grpc.Metadata();
            this.stub.delete({ key, timestamp: 0, nodeId: "" }, meta, { deadline: this.deadline() }, (err) => { if (err)
                reject(err);
            else
                resolve(); });
        });
    }
    /**
     * Close the underlying gRPC channel.  The client must not be used after
     * this call.
     */
    close() {
        this.stub.close();
    }
    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------
    /** Returns a deadline `Date` based on the configured per-call timeout. */
    deadline() {
        return new Date(Date.now() + this.timeoutMs);
    }
}
exports.DkvClient = DkvClient;
//# sourceMappingURL=client.js.map