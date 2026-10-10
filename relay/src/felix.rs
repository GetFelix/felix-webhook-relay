//! The relay's Felix connection: idempotent appends and the caches.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_client::{ClientConfig, ClusterClient, IdempotentProducer, StartPosition, TokenProvider};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use tokio::sync::{mpsc, oneshot};

use crate::config::Config;

/// How often one batch is re-sent before the producer is replaced.
const APPEND_ATTEMPTS: u32 = 5;
/// The most appends gathered into one batch.
const MAX_BATCH: usize = 256;

/// How many subscriptions one reads connection opens before it is replaced.
/// Felix's default is 1,024 streams to a connection.
const READS_PER_CONNECTION: usize = 400;

/// Connections the delivery tasks' group polls and acknowledgements are
/// spread over. Every idle endpoint holds a waiting poll, which holds one of
/// a connection's 1,024 streams, so a thousand endpoints on one connection
/// would leave the busy ones queueing for a stream.
const GROUP_CONNECTIONS: usize = 4;

pub(crate) struct Felix {
    client: Arc<ClusterClient>,
    groups: Vec<Arc<ClusterClient>>,
    /// The connection short reads go through: tails, single records, ranges.
    /// A dropped Felix subscription keeps its stream until the broker next
    /// writes to it, which on a quiet stream is never, so this connection is
    /// replaced every few hundred subscriptions and dropping it frees them.
    reads: tokio::sync::Mutex<(Arc<ClusterClient>, usize)>,
    config: Config,
    tokens: Arc<dyn TokenProvider>,
    /// The Felix tenant and the namespace every stream, group and cache lives in.
    pub(crate) tenant: String,
    pub(crate) namespace: String,
    appends: mpsc::Sender<Pending>,
}

impl Felix {
    pub(crate) async fn connect(
        config: &Config,
        namespace: &str,
        tokens: Arc<dyn TokenProvider>,
    ) -> Result<Self> {
        let client = Arc::new(open(config, Arc::clone(&tokens)).await?);
        let reads = Arc::new(open(config, Arc::clone(&tokens)).await?);
        let mut groups = Vec::new();
        for _ in 0..GROUP_CONNECTIONS {
            groups.push(Arc::new(open(config, Arc::clone(&tokens)).await?));
        }
        let (appends, waiting) = mpsc::channel(MAX_BATCH * 4);
        tokio::spawn(appender(
            Arc::clone(&client),
            config.felix_tenant.clone(),
            namespace.to_string(),
            waiting,
        ));
        Ok(Self {
            client,
            groups,
            reads: tokio::sync::Mutex::new((reads, 0)),
            config: config.clone(),
            tokens,
            tenant: config.felix_tenant.clone(),
            namespace: namespace.to_string(),
            appends,
        })
    }

    /// The reads connection, counting the `subscriptions` the caller opens.
    async fn reader(&self, subscriptions: usize) -> Result<Arc<ClusterClient>> {
        let mut reads = self.reads.lock().await;
        if reads.1 + subscriptions > READS_PER_CONNECTION {
            *reads = (
                Arc::new(open(&self.config, Arc::clone(&self.tokens)).await?),
                0,
            );
        }
        reads.1 += subscriptions;
        Ok(Arc::clone(&reads.0))
    }

    pub(crate) fn client(&self) -> &Arc<ClusterClient> {
        &self.client
    }

    /// The connection one endpoint's group traffic goes through.
    pub(crate) fn group_client(&self, endpoint: &str) -> &ClusterClient {
        let index = felix_relay_core::catalog::owner(endpoint, GROUP_CONNECTIONS as u32);
        &self.groups[index as usize]
    }

    pub(crate) async fn cache_get(&self, cache: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let value = self
            .client
            .client()
            .await
            .cache_get(&self.tenant, &self.namespace, cache, key)
            .await
            .with_context(|| format!("read {cache}/{key}"))?;
        Ok(value.map(|bytes| bytes.to_vec()))
    }

    pub(crate) async fn cache_put(
        &self,
        cache: &str,
        key: &str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let ttl_ms = ttl.map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX));
        self.client
            .client()
            .await
            .cache_put(
                &self.tenant,
                &self.namespace,
                cache,
                key,
                value.into(),
                ttl_ms,
            )
            .await
            .with_context(|| format!("write {cache}/{key}"))
    }

    pub(crate) async fn cache_delete(&self, cache: &str, key: &str) -> Result<()> {
        self.client
            .client()
            .await
            .cache_delete(&self.tenant, &self.namespace, cache, key)
            .await
            .with_context(|| format!("delete {cache}/{key}"))?;
        Ok(())
    }

    /// Add to a counter in `stats` without waiting. Counters are for the
    /// dashboard: Felix counts a retried add twice, so they are approximate.
    pub(crate) fn count(self: &Arc<Self>, key: String) {
        let felix = Arc::clone(self);
        tokio::spawn(async move {
            let client = felix.client.client().await;
            let added = client
                .counter_add(&felix.tenant, &felix.namespace, "stats", &key, 1)
                .await;
            if let Err(err) = added {
                tracing::debug!(%key, "not counted: {err:#}");
            }
        });
    }

    pub(crate) async fn counter(&self, key: &str) -> Result<i64> {
        let client = self.client.client().await;
        let value = client
            .counter_get(&self.tenant, &self.namespace, "stats", key)
            .await?;
        Ok(value.unwrap_or(0))
    }

    /// The oldest offset `stream` still holds, and the offset the next record
    /// appended to it will get.
    pub(crate) async fn bounds(&self, stream: &str) -> Result<(u64, u64)> {
        let reads = self.reader(2).await?;
        let subscribe =
            |start| reads.subscribe_from(&self.tenant, &self.namespace, stream, Some(start));
        let oldest = subscribe(StartPosition::Earliest).await?.start_offset();
        let tail = subscribe(StartPosition::Latest)
            .await?
            .live_offset()
            .with_context(|| format!("{stream} reported no tail"))?;
        Ok((oldest.unwrap_or(0).min(tail), tail))
    }

    /// Up to `limit` records of `stream` in `[from, to)`, in order. The
    /// broker skips some offsets, so fewer can come back than the range is wide.
    pub(crate) async fn read(
        &self,
        stream: &str,
        from: u64,
        to: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut records = Vec::new();
        if from >= to {
            return Ok(records);
        }
        let mut subscription = self
            .reader(1)
            .await?
            .subscribe_from(
                &self.tenant,
                &self.namespace,
                stream,
                Some(StartPosition::Offset(from)),
            )
            .await?;
        // The records are already in the log, so a pause this long means
        // the rest of the range is offsets the broker skips.
        while let Ok(event) =
            tokio::time::timeout(Duration::from_secs(1), subscription.next_event()).await
        {
            let Some(event) = event? else { break };
            let offset = event.offset.context("a record without an offset")?;
            if offset >= to {
                break;
            }
            records.push((offset, event.payload.to_vec()));
            if offset + 1 >= to || records.len() >= limit {
                break;
            }
        }
        Ok(records)
    }

    /// Publish without waiting for the broker to store it, for records that
    /// are a trail rather than a source of truth.
    pub(crate) async fn publish_unacked(&self, stream: &str, payload: Vec<u8>) -> Result<()> {
        self.client
            .publish(
                &self.tenant,
                &self.namespace,
                stream,
                payload,
                felix_wire::AckMode::None,
            )
            .await
            .with_context(|| format!("publish to {stream}"))?;
        Ok(())
    }

    /// Append one record to `stream` exactly once and return its offset.
    ///
    /// The append is handed to the connection's appender task, so a caller
    /// that is dropped midway, such as an HTTP handler whose sender hung up,
    /// cannot cancel an idempotent publish halfway, which would stop the
    /// producer. Appends that arrive while one is in flight go out together
    /// as one batch under one sequence number.
    pub(crate) async fn append(&self, stream: String, payload: Vec<u8>) -> Result<u64> {
        let (done, answer) = oneshot::channel();
        self.appends
            .send(Pending {
                stream,
                payload,
                done,
            })
            .await
            .map_err(|_| anyhow::anyhow!("the appender stopped"))?;
        answer.await.context("the appender stopped")?
    }
}

/// One append waiting for its turn.
struct Pending {
    stream: String,
    payload: Vec<u8>,
    done: oneshot::Sender<Result<u64>>,
}

/// Take appends as they come and publish what has gathered, per stream, in
/// arrival order.
async fn appender(
    client: Arc<ClusterClient>,
    tenant: String,
    namespace: String,
    mut appends: mpsc::Receiver<Pending>,
) {
    let mut producer = None;
    while let Some(first) = appends.recv().await {
        let mut gathered = vec![first];
        while gathered.len() < MAX_BATCH {
            match appends.try_recv() {
                Ok(next) => gathered.push(next),
                Err(_) => break,
            }
        }
        let mut streams: Vec<(String, Vec<Pending>)> = Vec::new();
        for pending in gathered {
            match streams
                .iter_mut()
                .find(|(stream, _)| *stream == pending.stream)
            {
                Some((_, batch)) => batch.push(pending),
                None => streams.push((pending.stream.clone(), vec![pending])),
            }
        }
        for (stream, batch) in streams {
            let payloads = batch.iter().map(|p| p.payload.clone()).collect();
            let result = publish_batch(
                &client,
                &mut producer,
                &tenant,
                &namespace,
                &stream,
                payloads,
            )
            .await;
            for (index, pending) in batch.into_iter().enumerate() {
                let answer = match &result {
                    Ok(first) => Ok(first + index as u64),
                    Err(err) => Err(anyhow::anyhow!("{err:#}")),
                };
                let _ = pending.done.send(answer);
            }
        }
    }
}

/// Publish one batch, re-sending it a few times, and return its first offset.
/// The records of a batch land together, at consecutive offsets.
async fn publish_batch(
    client: &Arc<ClusterClient>,
    producer: &mut Option<IdempotentProducer>,
    tenant: &str,
    namespace: &str,
    stream: &str,
    payloads: Vec<Vec<u8>>,
) -> Result<u64> {
    if producer.is_none() {
        *producer = Some(client.idempotent_producer().await?);
    }
    let current = producer.as_ref().expect("set above");
    let mut last = None;
    for attempt in 0..APPEND_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
        }
        // Re-sending the same batch after an error lands it at most once,
        // which is the whole point of the producer.
        match current
            .publish_batch(tenant, namespace, stream, payloads.clone())
            .await
        {
            Ok(Some(offset)) => return Ok(offset),
            Ok(None) => bail!("Felix acknowledged the append without an offset"),
            Err(err) => {
                tracing::warn!(%stream, attempt, "append failed: {err:#}");
                last = Some(err);
            }
        }
    }
    // The batch is in doubt and this producer refuses anything else until it
    // lands, so start over with a new one for the next batch.
    *producer = None;
    Err(last
        .expect("at least one attempt")
        .context("append to Felix"))
}

/// A connection to the brokers with the configured trust and tokens.
async fn open(config: &Config, tokens: Arc<dyn TokenProvider>) -> Result<ClusterClient> {
    let roots = match &config.ca_file {
        Some(path) => {
            let mut roots = RootCertStore::empty();
            for cert in CertificateDer::pem_file_iter(path)
                .with_context(|| format!("read {}", path.display()))?
            {
                roots.add(cert.context("parse broker CA certificate")?)?;
            }
            Some(Arc::new(roots))
        }
        None => None,
    };
    let quic = felix_client::quic_client_config(roots, true)?;
    let mut client_config = ClientConfig::optimized_defaults(quic);
    client_config.auth_tenant_id = Some(config.felix_tenant.clone());
    client_config.token_provider = Some(tokens);
    let mut brokers = Vec::new();
    for broker in &config.brokers {
        brokers.extend(
            tokio::net::lookup_host(broker.as_str())
                .await
                .with_context(|| format!("resolve broker {broker}"))?,
        );
    }
    ClusterClient::connect(&brokers, &config.server_name, client_config)
        .await
        .context("connect to Felix")
}
