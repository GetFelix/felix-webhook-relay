# felix-webhook-relay

Felix Webhook Relay on Kubernetes, installed next to a release of the
[felix chart](https://github.com/GetFelix/felix/tree/main/deploy/helm/felix).

| Component | Shape | Why |
| --- | --- | --- |
| Intake | Deployment, 2 replicas by default, Service, optional Ingress on `/in/` | It holds no state, so any replica takes any webhook |
| Delivery | StatefulSet, Parallel, headless Service | The pod's ordinal is `RELAY_WORKER_INDEX`, and a StatefulSet never runs two pods with one ordinal |
| Admin | Deployment, Service, optional Ingress | The page and the JSON API; every request is signed in |
| `tokens` | Deployment of one, Service, and a Role that may write two Secrets | Seeds Felix at every install and upgrade, signs the relay's service accounts in because Felix issues tokens only in exchange for an IdP token ([felix#954](https://github.com/GetFelix/felix/issues/954)), and keeps the relay's IdP token fresh in a Secret |

[docs/self-hosting.md](../../../docs/self-hosting.md#kubernetes) has the
install sequence, which interleaves this chart with the felix chart because
the brokers need the credential the tokens service mints. `ci/` holds the
values CI installs both charts with on kind, and `ci/kind-install.sh` is that
sequence as a script.

## Values

`values.yaml` documents each one. The ones an install must set:

| Value | Meaning |
| --- | --- |
| `felix.controlPlaneUrl` | The Felix control plane's API, e.g. `http://felix-controlplane:8443` |
| `felix.brokers` | Broker addresses as `host:port`, e.g. `[felix-broker:5000]` |
| `felix.serverName`, `felix.caSecret` | The name on the brokers' client certificate, and a Secret with the PEM to trust it by |
| `tokens.bootstrapUrl`, `tokens.bootstrapSecret` | The control plane's bootstrap listener and a Secret with its token |
| `secretKey.existingSecret` | A Secret with `RELAY_SECRET_KEY` |
| `oidc.issuer`, `oidc.clientSecret.existingSecret` | The provider admins sign in with, and a Secret with the relay's client secret there |
| `tenants`, `tenantAdmins` | Relay tenants, and who administers each |
| `admin.ingress`, `intake.ingress` | The hosts browsers and senders use, with TLS Secrets |
