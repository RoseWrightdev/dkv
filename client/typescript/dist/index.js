"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.tlsCredentials = exports.insecureCredentials = exports.Client = void 0;
var client_1 = require("./client");
Object.defineProperty(exports, "Client", { enumerable: true, get: function () { return client_1.Client; } });
var server_1 = require("./server");
Object.defineProperty(exports, "insecureCredentials", { enumerable: true, get: function () { return server_1.insecureCredentials; } });
Object.defineProperty(exports, "tlsCredentials", { enumerable: true, get: function () { return server_1.tlsCredentials; } });
//# sourceMappingURL=index.js.map