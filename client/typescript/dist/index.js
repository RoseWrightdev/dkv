"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.tlsCredentials = exports.insecureCredentials = exports.DkvClient = void 0;
var client_1 = require("./client");
Object.defineProperty(exports, "DkvClient", { enumerable: true, get: function () { return client_1.DkvClient; } });
var server_1 = require("./server");
Object.defineProperty(exports, "insecureCredentials", { enumerable: true, get: function () { return server_1.insecureCredentials; } });
Object.defineProperty(exports, "tlsCredentials", { enumerable: true, get: function () { return server_1.tlsCredentials; } });
//# sourceMappingURL=index.js.map