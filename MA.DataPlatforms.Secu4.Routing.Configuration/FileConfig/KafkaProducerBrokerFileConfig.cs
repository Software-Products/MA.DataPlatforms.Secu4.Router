// <copyright file="KafkaProducerBrokerFileConfig.cs" company="Motion Applied Ltd.">
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

namespace MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;

/// <summary>
/// Kafka-specific broker settings for a producer. Maps directly to
/// Confluent.Kafka <c>ProducerConfig</c>. These settings are entirely
/// independent of routing topology and would be replaced if the broker
/// type changes to something other than Kafka.
/// </summary>
public class KafkaProducerBrokerFileConfig
{
    /// <summary>
    /// Comma-separated list of host:port broker addresses.
    /// Default: <c>localhost:9092</c>.
    /// </summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>
    /// Optional logical client name included in broker logs for diagnostics.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Compression codec for messages. One of: <c>none</c>, <c>gzip</c>,
    /// <c>snappy</c>, <c>lz4</c>, <c>zstd</c>. Default: <c>none</c>.
    /// Mapped to integer: 0=None, 1=Gzip, 2=Snappy, 3=Lz4, 4=Zstd.
    /// </summary>
    public string CompressionType { get; set; } = "none";

    /// <summary>
    /// Codec-specific compression level. -1 = codec default.
    /// Gzip/Zstd: 0–9. Lz4: 0–12. Default: <c>-1</c>.
    /// </summary>
    public int CompressionLevel { get; set; } = -1;

    /// <summary>
    /// Delay in milliseconds to wait for messages to accumulate in the
    /// local queue before sending a batch. Higher values improve throughput
    /// at the cost of latency. Default: <c>20</c>.
    /// </summary>
    public int LingerMs { get; set; } = 20;

    /// <summary>
    /// Maximum size of a single request batch sent to the broker (bytes).
    /// Default: <c>4194304</c> (4 MB).
    /// </summary>
    public int BatchSize { get; set; } = 4 * 1024 * 1024;

    /// <summary>
    /// Maximum number of messages in the local producer queue. 0 = library
    /// default (100 000). Default: <c>0</c>.
    /// </summary>
    public int QueueBufferingMaxMessages { get; set; } = 0;

    /// <summary>
    /// Maximum total size of the local producer queue in kilobytes. 0 = library
    /// default (1 048 576 KB / 1 GB). Default: <c>0</c>.
    /// </summary>
    public int QueueBufferingMaxKbytes { get; set; } = 0;

    /// <summary>
    /// Maximum Kafka protocol message size in bytes. Should be ≤ broker
    /// <c>message.max.bytes</c>. Default: <c>1000000</c>.
    /// </summary>
    public int MessageMaxBytes { get; set; } = 1000000;

    /// <summary>
    /// Local message delivery timeout in milliseconds. Messages not delivered
    /// within this time are failed. Default: <c>300000</c> (5 min).
    /// </summary>
    public int MessageTimeoutMs { get; set; } = 300000;

    /// <summary>
    /// Request timeout in milliseconds waiting for broker acknowledgement.
    /// Default: <c>30000</c>.
    /// </summary>
    public int RequestTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Number of acknowledgements required before a produce request is
    /// considered successful. Values: <c>"0"</c>, <c>"1"</c>, <c>"all"</c>.
    /// Default: <c>all</c>.
    /// </summary>
    public string Acks { get; set; } = "all";

    /// <summary>
    /// Maximum in-flight requests per broker connection. Setting to 1 with
    /// EnableIdempotence=true guarantees ordering. Default: <c>1000000</c>.
    /// </summary>
    public int MaxInFlight { get; set; } = 1000000;

    /// <summary>
    /// Enables exactly-once semantics. Requires Acks=all and MaxInFlight=1.
    /// Default: <c>false</c>.
    /// </summary>
    public bool EnableIdempotence { get; set; } = false;

    /// <summary>
    /// Optional transactional producer ID. Required for transactions.
    /// Default: <c>null</c> (transactions disabled).
    /// </summary>
    public string? TransactionalId { get; set; }

    /// <summary>
    /// Initial backoff in ms between message send retry attempts.
    /// Default: <c>100</c>.
    /// </summary>
    public int RetryBackoffMs { get; set; } = 100;

    /// <summary>
    /// Maximum backoff in ms between retry attempts (exponential backoff
    /// capped here). Default: <c>1000</c>.
    /// </summary>
    public int RetryBackoffMaxMs { get; set; } = 1000;

    /// <summary>
    /// Maximum number of retries for a failed message send. Default is
    /// effectively unlimited: <c>2147483647</c>.
    /// </summary>
    public int MessageSendMaxRetries { get; set; } = 2147483647;

    /// <summary>
    /// How long in ms to cache the broker address before re-resolving DNS.
    /// Default: <c>1000</c>.
    /// </summary>
    public int BrokerAddressTtl { get; set; } = 1000;

    /// <summary>
    /// Initial backoff in ms before reconnecting after a connection failure.
    /// Default: <c>100</c>.
    /// </summary>
    public int ReconnectBackoffMs { get; set; } = 100;

    /// <summary>
    /// Maximum backoff in ms between reconnection attempts.
    /// Default: <c>10000</c>.
    /// </summary>
    public int ReconnectBackoffMaxMs { get; set; } = 10000;

    /// <summary>
    /// Timeout in ms for socket reads. Outstanding requests fail after this.
    /// Default: <c>60000</c>.
    /// </summary>
    public int SocketTimeoutMs { get; set; } = 60000;

    /// <summary>
    /// Enable TCP keepalives on broker sockets. Default: <c>false</c>.
    /// </summary>
    public bool SocketKeepaliveEnable { get; set; } = false;

    /// <summary>
    /// Background topic/broker metadata refresh interval in ms.
    /// Default: <c>300000</c>.
    /// </summary>
    public int TopicMetadataRefreshIntervalMs { get; set; } = 300000;

    /// <summary>
    /// Maximum age in ms of locally cached metadata before a forced refresh.
    /// Default: <c>900000</c>.
    /// </summary>
    public int MetadataMaxAgeMs { get; set; } = 900000;

    /// <summary>
    /// Interval in ms for emitting internal librdkafka statistics. 0 = disabled.
    /// Default: <c>0</c>.
    /// </summary>
    public int StatisticsIntervalMs { get; set; } = 0;

    /// <summary>Authentication and authorisation settings.</summary>
    public KafkaBrokerSecurityFileConfig Security { get; set; } = new();
}
