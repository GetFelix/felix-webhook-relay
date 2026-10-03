//! The relay's Felix connection: idempotent appends and the caches.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use felix_client::{ClientConfig, ClusterClient, IdempotentProducer, StartPosition};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use tokio::sync::Mutex;

use crate::config::Config;

/// How often one append is re-sent before the producer is replaced.
const APPEND_ATTEMPTS: u32 = 5;

pub(crate) struct Felix {
    /// Lives as long as the process, which lets the producer borrow it.
    client: &'static Arc<ClusterClient>,
    /// The Felix tenant and the namespace every stream, group and cache lives in.
    pub(crate) tenant: String,
    pub(crate) namespace: String,
    producer: Mutex<Option<IdempotentProducer<'static>>>,
}

impl Felix {
    pub(crate) async fn connect(config: &Config) -> Result<Self> {
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
        let token = std::fs::read_to_string(&config.token_file)
            .with_context(|| format!("read {}", config.token_file.display()))?;

        let quic = felix_client::quic_client_config(roots, true)?;
        let mut client_config = ClientConfig::optimized_defaults(quic);
        client_config.auth_tenant_id = Some(config.felix_tenant.clone());
        client_config.auth_token = Some(token.trim().to_string());
        let client = ClusterClient::connect(&config.brokers, &config.server_name, client_config)
            .await
            .context("connect to Felix")?;
        Ok(Self {
            client: Box::leak(Box::new(Arc::new(client))),
            tenant: config.felix_tenant.clone(),
            namespace: config.tenant.clone(),
            producer: Mutex::new(None),
        })
    }

    pub(crate) fn client(&self) -> &'static Arc<ClusterClient> {
        self.client
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

    /// The oldest offset `stream` still holds, and the offset the next record
    /// appended to it will get.
    pub(crate) async fn bounds(&self, stream: &str) -> Result<(u64, u64)> {
        let subscribe = |start| {
            self.client
                .subscribe_from(&self.tenant, &self.namespace, stream, Some(start))
        };
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
            .client
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
    /// Runs on its own task: dropping an idempotent publish midway stops the
    /// producer, and an HTTP handler is dropped whenever its sender hangs up.
    pub(crate) async fn append(self: &Arc<Self>, stream: String, payload: Vec<u8>) -> Result<u64> {
        let felix = Arc::clone(self);
        tokio::spawn(async move { felix.append_inner(&stream, payload).await })
            .await
            .context("append task")?
    }

    async fn append_inner(&self, stream: &str, payload: Vec<u8>) -> Result<u64> {
        let mut producer = self.producer.lock().await;
        if producer.is_none() {
            *producer = Some(self.client.idempotent_producer().await?);
        }
        let current = producer.as_ref().expect("set above");
        let mut last = None;
        for attempt in 0..APPEND_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
            }
            // Re-sending the same payload after an error lands it at most
            // once, which is the whole point of the producer.
            match current
                .publish(&self.tenant, &self.namespace, stream, payload.clone())
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
        // The batch is in doubt and this producer refuses anything else until
        // it lands, so start over with a new one for the next webhook.
        *producer = None;
        Err(last
            .expect("at least one attempt")
            .context("append to Felix"))
    }
}
