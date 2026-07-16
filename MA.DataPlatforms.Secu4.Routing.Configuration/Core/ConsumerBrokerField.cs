// <copyright file="ConsumerBrokerFields.cs" company="Motion Applied Ltd.">
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
/// Identifies individual Kafka consumer broker settings that can be selectively
/// excluded from the file override in
/// <see cref="JsonKafkaConsumerBrokerOverrideProvider"/>.
///
/// Pass a combination of flags to <c>excludeFields</c> to keep those values
/// from the wrapped inner provider instead of reading them from the JSON file.
///
/// <code>
/// // Keep bootstrap servers and group ID from the existing provider;
/// // override security and session timeouts from the file.
/// new JsonKafkaConsumerBrokerOverrideProvider(
///     inner: existing,
///     excludeFields: ConsumerBrokerFields.BootstrapServers
///                  | ConsumerBrokerFields.GroupId);
/// </code>
/// </summary>
[Flags]
public enum ConsumerBrokerFields : long
{
    /// <summary>Override all fields from the JSON file (default behaviour).</summary>
    None = 0,

    // ── Connection ───────────────────────────────────────────────────────────

    /// <summary>Comma-separated <c>host:port</c> broker addresses.</summary>
    BootstrapServers = 1L << 0,

    /// <summary>Consumer group identifier.</summary>
    GroupId = 1L << 1,

    /// <summary>Optional logical client name included in broker logs.</summary>
    ClientId = 1L << 2,

    // ── Offset commit ─────────────────────────────────────────────────────────

    /// <summary>Automatically commit consumed offsets.</summary>
    EnableAutoCommit = 1L << 3,

    /// <summary>Frequency in ms at which offsets are auto-committed.</summary>
    AutoCommitIntervalMs = 1L << 4,

    // ── Session & heartbeat ───────────────────────────────────────────────────

    /// <summary>Heartbeat interval in ms. Should be ≤ <c>SessionTimeoutMs / 3</c>.</summary>
    HeartbeatIntervalMs = 1L << 5,

    /// <summary>Timeout in ms used to detect consumer failures.</summary>
    SessionTimeoutMs = 1L << 6,

    /// <summary>Maximum allowed time between <c>Consume()</c> calls before the consumer is evicted.</summary>
    MaxPollIntervalMs = 1L << 7,

    // ── Fetch tuning ──────────────────────────────────────────────────────────

    /// <summary>Maximum bytes returned by the broker per fetch request.</summary>
    FetchMaxBytes = 1L << 8,

    /// <summary>Maximum bytes per partition per fetch request.</summary>
    MaxPartitionFetchBytes = 1L << 9,

    // ── Security ──────────────────────────────────────────────────────────────

    /// <summary>The entire security block (protocol, SASL mechanism, credentials, SSL paths).</summary>
    Security = 1L << 10,

    // ── Fetch tuning (extended) ───────────────────────────────────────────────

    /// <summary>Minimum data the broker should accumulate before responding to a fetch request (bytes).</summary>
    FetchMinBytes = 1L << 11,

    /// <summary>Maximum time in ms the broker may wait to accumulate FetchMinBytes of data.</summary>
    FetchWaitMaxMs = 1L << 12,

    /// <summary>How long in ms to cache the broker address before re-resolving DNS.</summary>
    BrokerAddressTtl = 1L << 13,

    /// <summary>Minimum number of messages per partition queued locally before fetching more.</summary>
    QueuedMinMessages = 1L << 14,

    /// <summary>Maximum kilobytes of messages queued locally per partition.</summary>
    QueuedMaxMessagesKbytes = 1L << 15,

    /// <summary>How long in ms to wait before retrying a failed fetch request.</summary>
    FetchErrorBackoffMs = 1L << 16,

    // ── Transport & reconnect ─────────────────────────────────────────────────

    /// <summary>Initial backoff duration in ms before reconnecting after a connection failure.</summary>
    ReconnectBackoffMs = 1L << 17,

    /// <summary>Maximum backoff in ms between reconnection attempts.</summary>
    ReconnectBackoffMaxMs = 1L << 18,

    /// <summary>Timeout in ms for socket reads.</summary>
    SocketTimeoutMs = 1L << 19,

    /// <summary>Enable TCP keepalives on broker sockets.</summary>
    SocketKeepaliveEnable = 1L << 20,

    // ── Metadata ─────────────────────────────────────────────────────────────

    /// <summary>How often in ms the topic and broker metadata is refreshed in the background.</summary>
    TopicMetadataRefreshIntervalMs = 1L << 21,

    /// <summary>Maximum age in ms of locally cached metadata before a forced refresh.</summary>
    MetadataMaxAgeMs = 1L << 22,

    // ── Observability ─────────────────────────────────────────────────────────

    /// <summary>Interval in ms for emitting internal librdkafka statistics. 0 = disabled.</summary>
    StatisticsIntervalMs = 1L << 23,

    /// <summary>Exclude all fields — the JSON file has no effect.</summary>
    All = (1L << 24) - 1,
}
