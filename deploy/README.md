# Usage Guide

## 1. Building the Docker Image

To build the oryx container image, run the following command from the root of the project:

```bash
docker build -t oryx:latest -f deploy/Dockerfile .
```

To run it locally in Docker for development purposes:

```bash
docker run -p 50051:50051 -p 7946:7946 oryx:latest
```

---

## 2. Deploying to Kubernetes with Helm

### Installation

To install the chart in your Kubernetes cluster under the release name `oryx`:

```bash
helm install oryx ./deploy/charts/oryx
```

### Upgrade / Configuration

To upgrade your chart or tweak properties:

```bash
helm upgrade oryx ./deploy/charts/oryx -f deploy/charts/oryx/values.yaml
```

To uninstall and clean up all resources:

```bash
helm uninstall oryx
```

---

## 3. Client Connection Guide inside Kubernetes

Once deployed, the chart creates two key services in your namespace:
1. ClusterIP Service (`oryx`): Provides single-point load balancing over the cluster.
2. Headless Service (`oryx-headless`): Resolves directly to the individual replica IPs (useful for cluster discovery, client-side ring hashing, or direct target writes).

### Python Client Integration

If your microservice is running inside the same Kubernetes namespace, you can connect directly to the service using the `oryx` Python client.

#### A. Connect to the Load-Balanced Service
Use this for simple read/write workloads where standard Kubernetes load balancing is sufficient.

```python
from oryx import OryxClient, insecure_credentials

# Standard Kubernetes service DNS resolving to the ClusterIP
address = "oryx.default.svc.cluster.local:50051"

with OryxClient.connect(address, insecure_credentials()) as client:
    client.set("foo", b"bar")
    value = client.get("foo")
    print(f"Retrieved: {value}")
```

#### B. Connect to a Specific StatefulSet Pod (Direct Sharding/Addressing)
For master-replica writes, sharding, or specific debugging, you can direct traffic to individual pods (`oryx-0`, `oryx-1`, `oryx-2`) using the headless service DNS.

```python
from oryx import OryxClient, insecure_credentials

# Explicitly address pod 0 directly
address = "oryx-0.oryx-headless.default.svc.cluster.local:50051"

with OryxClient.connect(address, insecure_credentials()) as client:
    client.set("pod-specific-key", b"direct-write")
```
