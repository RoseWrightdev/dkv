import { BinaryReader, BinaryWriter } from "@bufbuild/protobuf/wire";
import { type CallOptions, type ChannelCredentials, Client, type ClientOptions, type ClientUnaryCall, type handleUnaryCall, type Metadata, type ServiceError, type UntypedServiceImplementation } from "@grpc/grpc-js";
export declare const protobufPackage = "dkv";
export interface PushRequest {
    entries: SetRequest[];
    deletions: DeleteRequest[];
}
export interface PushResponse {
}
export interface PullRequest {
    rootDigest: number;
    shardDigests: {
        [key: number]: number;
    };
    subDigests: {
        [key: number]: ShardDigests;
    };
    nodeId: string;
}
export interface PullRequest_ShardDigestsEntry {
    key: number;
    value: number;
}
export interface PullRequest_SubDigestsEntry {
    key: number;
    value: ShardDigests | undefined;
}
export interface ShardDigests {
    subHashes: number[];
}
export interface PullResponse {
    entries: SetRequest[];
    deletions: DeleteRequest[];
}
export interface WalEntry {
    set?: SetRequest | undefined;
    delete?: DeleteRequest | undefined;
}
export interface GetRequest {
    key: string;
}
export interface GetResponse {
    value: Buffer;
    exists: boolean;
}
export interface SetRequest {
    key: string;
    value: Buffer;
    timestamp: number;
    nodeId: string;
}
export interface SetResponse {
}
export interface DeleteRequest {
    key: string;
    timestamp: number;
    nodeId: string;
}
export interface DeleteResponse {
}
export declare const PushRequest: MessageFns<PushRequest>;
export declare const PushResponse: MessageFns<PushResponse>;
export declare const PullRequest: MessageFns<PullRequest>;
export declare const PullRequest_ShardDigestsEntry: MessageFns<PullRequest_ShardDigestsEntry>;
export declare const PullRequest_SubDigestsEntry: MessageFns<PullRequest_SubDigestsEntry>;
export declare const ShardDigests: MessageFns<ShardDigests>;
export declare const PullResponse: MessageFns<PullResponse>;
export declare const WalEntry: MessageFns<WalEntry>;
export declare const GetRequest: MessageFns<GetRequest>;
export declare const GetResponse: MessageFns<GetResponse>;
export declare const SetRequest: MessageFns<SetRequest>;
export declare const SetResponse: MessageFns<SetResponse>;
export declare const DeleteRequest: MessageFns<DeleteRequest>;
export declare const DeleteResponse: MessageFns<DeleteResponse>;
export type DkvServiceService = typeof DkvServiceService;
export declare const DkvServiceService: {
    readonly get: {
        readonly path: "/dkv.DkvService/Get";
        readonly requestStream: false;
        readonly responseStream: false;
        readonly requestSerialize: (value: GetRequest) => Buffer;
        readonly requestDeserialize: (value: Buffer) => GetRequest;
        readonly responseSerialize: (value: GetResponse) => Buffer;
        readonly responseDeserialize: (value: Buffer) => GetResponse;
    };
    readonly set: {
        readonly path: "/dkv.DkvService/Set";
        readonly requestStream: false;
        readonly responseStream: false;
        readonly requestSerialize: (value: SetRequest) => Buffer;
        readonly requestDeserialize: (value: Buffer) => SetRequest;
        readonly responseSerialize: (value: SetResponse) => Buffer;
        readonly responseDeserialize: (value: Buffer) => SetResponse;
    };
    readonly delete: {
        readonly path: "/dkv.DkvService/Delete";
        readonly requestStream: false;
        readonly responseStream: false;
        readonly requestSerialize: (value: DeleteRequest) => Buffer;
        readonly requestDeserialize: (value: Buffer) => DeleteRequest;
        readonly responseSerialize: (value: DeleteResponse) => Buffer;
        readonly responseDeserialize: (value: Buffer) => DeleteResponse;
    };
    /** Anti-Entropy Sync */
    readonly pull: {
        readonly path: "/dkv.DkvService/Pull";
        readonly requestStream: false;
        readonly responseStream: false;
        readonly requestSerialize: (value: PullRequest) => Buffer;
        readonly requestDeserialize: (value: Buffer) => PullRequest;
        readonly responseSerialize: (value: PullResponse) => Buffer;
        readonly responseDeserialize: (value: Buffer) => PullResponse;
    };
    readonly push: {
        readonly path: "/dkv.DkvService/Push";
        readonly requestStream: false;
        readonly responseStream: false;
        readonly requestSerialize: (value: PushRequest) => Buffer;
        readonly requestDeserialize: (value: Buffer) => PushRequest;
        readonly responseSerialize: (value: PushResponse) => Buffer;
        readonly responseDeserialize: (value: Buffer) => PushResponse;
    };
};
export interface DkvServiceServer extends UntypedServiceImplementation {
    get: handleUnaryCall<GetRequest, GetResponse>;
    set: handleUnaryCall<SetRequest, SetResponse>;
    delete: handleUnaryCall<DeleteRequest, DeleteResponse>;
    /** Anti-Entropy Sync */
    pull: handleUnaryCall<PullRequest, PullResponse>;
    push: handleUnaryCall<PushRequest, PushResponse>;
}
export interface DkvServiceClient extends Client {
    get(request: GetRequest, callback: (error: ServiceError | null, response: GetResponse) => void): ClientUnaryCall;
    get(request: GetRequest, metadata: Metadata, callback: (error: ServiceError | null, response: GetResponse) => void): ClientUnaryCall;
    get(request: GetRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: ServiceError | null, response: GetResponse) => void): ClientUnaryCall;
    set(request: SetRequest, callback: (error: ServiceError | null, response: SetResponse) => void): ClientUnaryCall;
    set(request: SetRequest, metadata: Metadata, callback: (error: ServiceError | null, response: SetResponse) => void): ClientUnaryCall;
    set(request: SetRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: ServiceError | null, response: SetResponse) => void): ClientUnaryCall;
    delete(request: DeleteRequest, callback: (error: ServiceError | null, response: DeleteResponse) => void): ClientUnaryCall;
    delete(request: DeleteRequest, metadata: Metadata, callback: (error: ServiceError | null, response: DeleteResponse) => void): ClientUnaryCall;
    delete(request: DeleteRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: ServiceError | null, response: DeleteResponse) => void): ClientUnaryCall;
    /** Anti-Entropy Sync */
    pull(request: PullRequest, callback: (error: ServiceError | null, response: PullResponse) => void): ClientUnaryCall;
    pull(request: PullRequest, metadata: Metadata, callback: (error: ServiceError | null, response: PullResponse) => void): ClientUnaryCall;
    pull(request: PullRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: ServiceError | null, response: PullResponse) => void): ClientUnaryCall;
    push(request: PushRequest, callback: (error: ServiceError | null, response: PushResponse) => void): ClientUnaryCall;
    push(request: PushRequest, metadata: Metadata, callback: (error: ServiceError | null, response: PushResponse) => void): ClientUnaryCall;
    push(request: PushRequest, metadata: Metadata, options: Partial<CallOptions>, callback: (error: ServiceError | null, response: PushResponse) => void): ClientUnaryCall;
}
export declare const DkvServiceClient: {
    new (address: string, credentials: ChannelCredentials, options?: Partial<ClientOptions>): DkvServiceClient;
    service: typeof DkvServiceService;
    serviceName: string;
};
type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;
export type DeepPartial<T> = T extends Builtin ? T : T extends globalThis.Array<infer U> ? globalThis.Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>> : T extends {} ? {
    [K in keyof T]?: DeepPartial<T[K]>;
} : Partial<T>;
type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P : P & {
    [K in keyof P]: Exact<P[K], I[K]>;
} & {
    [K in Exclude<keyof I, KeysOfUnion<P>>]: never;
};
export interface MessageFns<T> {
    encode(message: T, writer?: BinaryWriter): BinaryWriter;
    decode(input: BinaryReader | Uint8Array, length?: number): T;
    fromJSON(object: any): T;
    toJSON(message: T): unknown;
    create<I extends Exact<DeepPartial<T>, I>>(base?: I): T;
    fromPartial<I extends Exact<DeepPartial<T>, I>>(object: I): T;
}
export {};
//# sourceMappingURL=dkv.d.ts.map