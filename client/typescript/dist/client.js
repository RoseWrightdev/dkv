"use strict";
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
exports.Client = void 0;
const grpc = __importStar(require("@grpc/grpc-js"));
const oryx_1 = require("./gen/api/oryx");
class Client {
    constructor(stub, timeoutMs) {
        this.stub = stub;
        this.timeoutMs = timeoutMs;
    }
    /**
     * Open a connection to a ORYX node.
     *
     * @param address      Host and port of the target ORYX node, e.g. `"localhost:50051"`.
     * @param credentials  Channel credentials — use `insecureCredentials()` for plaintext or `tlsCredentials()` for secure channels.
     * @param options      Optional configuration details including per-call timeoutMs and custom gRPC channelOptions.
     * @returns A connected `Client` instance.
     */
    static connect(address, credentials, options = {}) {
        const { timeoutMs = 5000, channelOptions = {} } = options;
        const stub = new oryx_1.OryxServiceClient(address, credentials, channelOptions);
        return new Client(stub, timeoutMs);
    }
    /**
     * Retrieve the value associated with `key`.
     *
     * @param key The unique identifier string whose value to retrieve.
     * @returns A promise resolving to a `GetResult` containing `{ value: Buffer | null, exists: boolean }`.
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
     * Store `value` under `key`.
     *
     * @param key   The unique identifier string under which to store the value.
     * @param value The raw data payload to store, passed as a Node.js `Buffer` or `Uint8Array`.
     * @returns A promise that resolves when the operation is successfully completed.
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
     * Remove `key` and its associated value from the store.
     *
     * @param key The unique identifier string to remove.
     * @returns A promise that resolves once deleted. Resolves even if the key did not exist in the store.
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
     * Close the underlying gRPC channel.
     * The client instance must not be used for any further operations after this call.
     */
    close() {
        this.stub.close();
    }
    /** Returns a deadline `Date` based on the configured per-call timeout. */
    deadline() {
        return new Date(Date.now() + this.timeoutMs);
    }
}
exports.Client = Client;
//# sourceMappingURL=client.js.map