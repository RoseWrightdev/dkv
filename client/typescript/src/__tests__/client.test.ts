/**
 * Unit tests for the dkv TypeScript client.
 *
 * The gRPC-generated stub (DkvServiceClient) is replaced with a Jest
 * mock so that no real network connection is required.  Each test drives the
 * mock's callback directly, letting us verify:
 *   - correct request payloads are forwarded to the stub
 *   - promise resolution for successful calls
 *   - promise rejection on gRPC errors
 *   - the deadline is computed from the configured timeoutMs
 *   - close() delegates to the stub
 */

import * as grpc from "@grpc/grpc-js";
import { describe, expect, it, jest } from '@jest/globals';
import type { Mocked, MockInstance } from 'jest-mock';
import { Client, ClientOptions } from "../client";
import type { DkvServiceClient as GrpcClient } from "../gen/api/dkv";

// Mock the generated gRPC client constructor

// We capture the stub instance created inside Client.connect so we can
// inspect / drive its methods from individual tests.
let mockStubInstance: Mocked<Pick<GrpcClient, "get" | "set" | "delete" | "close">>;

jest.mock("../gen/api/dkv", () => {
  const original = jest.requireActual("../gen/api/dkv");

  // Factory that builds a fresh stub mock and stores it for test access.
  const MockDkvServiceClient = jest.fn().mockImplementation(() => {
    mockStubInstance = {
      get: jest.fn(),
      set: jest.fn(),
      delete: jest.fn(),
      close: jest.fn(),
    } as unknown as Mocked<Pick<GrpcClient, "get" | "set" | "delete" | "close">>;
    return mockStubInstance;
  });

  return {
    ...(original as Record<string, unknown>),
    DkvServiceClient: MockDkvServiceClient,
  };
});

// Helpers

/**
 * Build a Client using insecure credentials and the supplied options.
 * A fresh stub mock is created as a side-effect of calling connect().
 */
function makeClient(options: ClientOptions = {}): Client {
  return Client.connect(
    "localhost:50051",
    grpc.credentials.createInsecure(),
    options,
  );
}

/**
 * Simulate the gRPC callback being invoked with a successful response.
 */
function resolveStubCall<T>(
  mockFn: MockInstance<(...args: any[]) => any>,
  response: T,
  callIndex = 0,
): void {
  const callArgs = mockFn.mock.calls[callIndex]!;
  // Callback is always the last argument.
  const callback: (err: null, res: T) => void = callArgs[callArgs.length - 1];
  callback(null, response);
}

/**
 * Simulate the gRPC callback being invoked with an error.
 */
function rejectStubCall(
  mockFn: MockInstance<(...args: any[]) => any>,
  error: grpc.ServiceError,
  callIndex = 0,
): void {
  const callArgs = mockFn.mock.calls[callIndex]!;
  const callback: (err: grpc.ServiceError, res: null) => void =
    callArgs[callArgs.length - 1];
  callback(error, null as any);
}

/** Build a minimal ServiceError. */
function makeGrpcError(message: string, code = grpc.status.INTERNAL): grpc.ServiceError {
  const err = new Error(message) as grpc.ServiceError;
  err.code = code;
  err.details = message;
  err.metadata = new grpc.Metadata();
  return err;
}

// connect()
describe("Client.connect", () => {
  it("returns a Client instance", () => {
    const client = makeClient();
    expect(client).toBeInstanceOf(Client);
  });

  it("applies the default 5 000 ms timeout when none is specified", async () => {
    const client = makeClient(); // no timeoutMs option

    const before = Date.now();
    const promise = client.get("any-key");

    // Peek at the deadline passed to the stub — it should be ~5 000 ms away.
    const callOptions = mockStubInstance.get.mock.calls[0]![2] as { deadline: Date };
    const deadlineMs = callOptions.deadline.getTime() - before;
    expect(deadlineMs).toBeGreaterThanOrEqual(4_900);
    expect(deadlineMs).toBeLessThanOrEqual(5_100);

    // Clean up the dangling promise.
    resolveStubCall(mockStubInstance.get, { exists: false, value: Buffer.alloc(0) });
    await promise;
  });

  it("applies a custom timeoutMs", async () => {
    const client = makeClient({ timeoutMs: 1_000 });

    const before = Date.now();
    const promise = client.get("any-key");

    const callOptions = mockStubInstance.get.mock.calls[0]![2] as { deadline: Date };
    const deadlineMs = callOptions.deadline.getTime() - before;
    expect(deadlineMs).toBeGreaterThanOrEqual(900);
    expect(deadlineMs).toBeLessThanOrEqual(1_100);

    resolveStubCall(mockStubInstance.get, { exists: false, value: Buffer.alloc(0) });
    await promise;
  });
});

// get()
describe("Client#get", () => {
  it("resolves with { value, exists: true } when the key exists", async () => {
    const client = makeClient();
    const expectedValue = Buffer.from("hello");

    const resultPromise = client.get("my-key");
    resolveStubCall(mockStubInstance.get, { exists: true, value: expectedValue });

    const result = await resultPromise;
    expect(result.exists).toBe(true);
    expect(result.value).toEqual(expectedValue);
  });

  it("resolves with { value: null, exists: false } when the key is absent", async () => {
    const client = makeClient();

    const resultPromise = client.get("missing-key");
    resolveStubCall(mockStubInstance.get, {
      exists: false,
      value: Buffer.alloc(0),
    });

    const result = await resultPromise;
    expect(result.exists).toBe(false);
    expect(result.value).toBeNull();
  });

  it("forwards the correct key in the request", async () => {
    const client = makeClient();
    const promise = client.get("target-key");
    resolveStubCall(mockStubInstance.get, { exists: false, value: Buffer.alloc(0) });
    await promise;

    const request = mockStubInstance.get.mock.calls[0]![0];
    expect(request).toMatchObject({ key: "target-key" });
  });

  it("rejects when the gRPC call returns an error", async () => {
    const client = makeClient();
    const err = makeGrpcError("not found", grpc.status.NOT_FOUND);

    const resultPromise = client.get("bad-key");
    rejectStubCall(mockStubInstance.get, err);

    await expect(resultPromise).rejects.toMatchObject({
      code: grpc.status.NOT_FOUND,
    });
  });

  it("passes a Metadata object and deadline to the stub", async () => {
    const client = makeClient();
    const promise = client.get("k");
    resolveStubCall(mockStubInstance.get, { exists: false, value: Buffer.alloc(0) });
    await promise;

    const [, meta, callOpts] = mockStubInstance.get.mock.calls[0]!;
    expect(meta).toBeInstanceOf(grpc.Metadata);
    expect((callOpts as { deadline: Date }).deadline).toBeInstanceOf(Date);
  });
});

// set()
describe("Client#set", () => {
  it("resolves on success", async () => {
    const client = makeClient();
    const promise = client.set("k", Buffer.from("v"));
    resolveStubCall(mockStubInstance.set, {});
    await expect(promise).resolves.toBeUndefined();
  });

  it("forwards the correct key and value in the request", async () => {
    const client = makeClient();
    const value = Buffer.from("world");
    const promise = client.set("hello", value);
    resolveStubCall(mockStubInstance.set, {});
    await promise;

    const request = mockStubInstance.set.mock.calls[0]![0];
    expect(request.key).toBe("hello");
    expect(Buffer.from(request.value)).toEqual(value);
  });

  it("accepts a Uint8Array value", async () => {
    const client = makeClient();
    const value = new Uint8Array([1, 2, 3]);
    const promise = client.set("k", value);
    resolveStubCall(mockStubInstance.set, {});
    await promise;

    const request = mockStubInstance.set.mock.calls[0]![0];
    expect(Buffer.from(request.value)).toEqual(Buffer.from(value));
  });

  it("rejects when the gRPC call returns an error", async () => {
    const client = makeClient();
    const err = makeGrpcError("unavailable", grpc.status.UNAVAILABLE);
    const promise = client.set("k", Buffer.from("v"));
    rejectStubCall(mockStubInstance.set, err);

    await expect(promise).rejects.toMatchObject({
      code: grpc.status.UNAVAILABLE,
    });
  });

  it("passes a Metadata object and deadline to the stub", async () => {
    const client = makeClient();
    const promise = client.set("k", Buffer.from("v"));
    resolveStubCall(mockStubInstance.set, {});
    await promise;

    const [, meta, callOpts] = mockStubInstance.set.mock.calls[0]!;
    expect(meta).toBeInstanceOf(grpc.Metadata);
    expect((callOpts as { deadline: Date }).deadline).toBeInstanceOf(Date);
  });
});

// delete()
describe("Client#delete", () => {
  it("resolves on success", async () => {
    const client = makeClient();
    const promise = client.delete("key-to-remove");
    resolveStubCall(mockStubInstance.delete, {});
    await expect(promise).resolves.toBeUndefined();
  });

  it("forwards the correct key in the request", async () => {
    const client = makeClient();
    const promise = client.delete("delete-me");
    resolveStubCall(mockStubInstance.delete, {});
    await promise;

    const request = mockStubInstance.delete.mock.calls[0]![0];
    expect(request.key).toBe("delete-me");
  });

  it("resolves even when the key did not exist (server still succeeds)", async () => {
    const client = makeClient();
    const promise = client.delete("phantom-key");
    // Server sends success for absent keys per the API contract.
    resolveStubCall(mockStubInstance.delete, {});
    await expect(promise).resolves.toBeUndefined();
  });

  it("rejects when the gRPC call returns an error", async () => {
    const client = makeClient();
    const err = makeGrpcError("internal", grpc.status.INTERNAL);
    const promise = client.delete("k");
    rejectStubCall(mockStubInstance.delete, err);

    await expect(promise).rejects.toMatchObject({
      code: grpc.status.INTERNAL,
    });
  });

  it("passes a Metadata object and deadline to the stub", async () => {
    const client = makeClient();
    const promise = client.delete("k");
    resolveStubCall(mockStubInstance.delete, {});
    await promise;

    const [, meta, callOpts] = mockStubInstance.delete.mock.calls[0]!;
    expect(meta).toBeInstanceOf(grpc.Metadata);
    expect((callOpts as { deadline: Date }).deadline).toBeInstanceOf(Date);
  });
});

// close()
describe("Client#close", () => {
  it("calls close() on the underlying stub", () => {
    const client = makeClient();
    client.close();
    expect(mockStubInstance.close).toHaveBeenCalledTimes(1);
  });
});
