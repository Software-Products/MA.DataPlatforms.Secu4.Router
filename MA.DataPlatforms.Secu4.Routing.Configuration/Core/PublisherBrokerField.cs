// <copyright file="PublisherBrokerFields.cs" company="Motion Applied Ltd.">
//
// Copyright 2025 Motion Applied Ltd
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
/// Identifies individual Kafka producer broker settings that can be selectively
/// excluded from the file override in
/// <see cref="JsonKafkaPublisherBrokerOverrideProvider"/>.
///
/// Pass a combination of flags to <c>excludeFields</c> to keep those values
/// from the wrapped inner provider instead of reading them from the JSON file.
///
/// <code>
/// // Keep bootstrap servers and client ID from the existing provider;
/// // override everything else (security, compression, batching…) from the file.
/// new JsonKafkaPublisherBrokerOverrideProvider(
///     inner: existing,
///     excludeFields: PublisherBrokerFields.BootstrapServers
///                  | PublisherBrokerFields.ClientId);
/// </code>
/// </summary>
[Flags]
public enum PublisherBrokerFields : long
{
    /// <summary>Override all fields from the JSON file (default behaviour).</summary>
    None = 0,

    // ── Connection ───────────────────────────────────────────────────────────

    /// <summary>Comma-separated <c>host:port</c> broker addresses.</summary>
    BootstrapServers = 1L << 0,

    /// <summary>Optional logical client name included in broker logs.</summary>
    ClientId = 1L << 1,

    // ── Compression ──────────────────────────────────────────────────────────

    /// <summary>Compression codec: <c>none</c>, <c>gzip</c>, <c>snappy</c>, <c>lz4</c>, <c>zstd</c>.</summary>
    CompressionType = 1L << 2,

    /// <summary>Codec-specific compression level. <c>-1</c> = codec default.</summary>
    CompressionLevel = 1L << 3,

    // ── Batching ─────────────────────────────────────────────────────────────

    /// <summary>Delay in ms to wait for messages to accumulate before sending a batch.</summary>
    LingerMs = 1L << 4,

    /// <summary>Maximum batch size in bytes.</summary>
    BatchSize = 1L << 5,

    /// <summary>Maximum number of messages in the local producer queue.</summary>
    QueueBufferingMaxMessages = 1L << 6,

    /// <summary>Maximum total size of the local producer queue in kilobytes.</summary>
    QueueBufferingMaxKbytes = 1L << 7,

    // ── Message limits & timeouts ─────────────────────────────────────────────

    /// <summary>Maximum Kafka protocol message size in bytes.</summary>
    MessageMaxBytes = 1L << 8,

    /// <summary>Local message delivery timeout in milliseconds.</summary>
    MessageTimeoutMs = 1L << 9,

    /// <summary>Request timeout waiting for broker acknowledgement (ms).</summary>
    RequestTimeoutMs = 1L << 10,

    // ── Reliability ───────────────────────────────────────────────────────────

    /// <summary>Required acknowledgements: <c>"0"</c>, <c>"1"</c>, or <c>"all"</c>.</summary>
    Acks = 1L << 11,

    /// <summary>Maximum in-flight requests per broker connection.</summary>
    MaxInFlight = 1L << 12,

    /// <summary>Enable exactly-once semantics.</summary>
    EnableIdempotence = 1L << 13,

    // ── Security ──────────────────────────────────────────────────────────────

    /// <summary>The entire security block (protocol, SASL mechanism, credentials, SSL paths).</summary>
    Security = 1L << 14,

    // ── Retry ─────────────────────────────────────────────────────────────────

    /// <summary>Initial backoff in ms between message send retry attempts.</summary>
    RetryBackoffMs = 1L << 15,

    /// <summary>Maximum backoff in ms between retry attempts (exponential backoff capped here).</summary>
    RetryBackoffMaxMs = 1L << 16,

    /// <summary>Maximum number of retries for a failed message send.</summary>
    MessageSendMaxRetries = 1L << 17,

    // ── Transport & reconnect ─────────────────────────────────────────────────

    /// <summary>How long in ms to cache the broker address before re-resolving DNS.</summary>
    BrokerAddressTtl = 1L << 18,

    /// <summary>Initial backoff in ms before reconnecting after a connection failure.</summary>
    ReconnectBackoffMs = 1L << 19,

    /// <summary>Maximum backoff in ms between reconnection attempts.</summary>
    ReconnectBackoffMaxMs = 1L << 20,

    /// <summary>Timeout in ms for socket reads.</summary>
    SocketTimeoutMs = 1L << 21,

    /// <summary>Enable TCP keepalives on broker sockets.</summary>
    SocketKeepaliveEnable = 1L << 22,

    // ── Metadata ─────────────────────────────────────────────────────────────

    /// <summary>Background topic/broker metadata refresh interval in ms.</summary>
    TopicMetadataRefreshIntervalMs = 1L << 23,

    /// <summary>Maximum age in ms of locally cached metadata before a forced refresh.</summary>
    MetadataMaxAgeMs = 1L << 24,

    // ── Observability ─────────────────────────────────────────────────────────

    /// <summary>Interval in ms for emitting internal librdkafka statistics. 0 = disabled.</summary>
    StatisticsIntervalMs = 1L << 25,

    /// <summary>Exclude all fields — the JSON file has no effect.</summary>
    All = (1L << 26) - 1,
}
