// <copyright file="KafkaConsumerBrokerFileConfig.cs" company="Motion Applied Ltd.">
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
/// Kafka-specific broker settings for a consumer. Maps directly to
/// Confluent.Kafka <c>ConsumerConfig</c>. These settings are entirely
/// independent of routing topology and would be replaced if the broker
/// type changes to something other than Kafka.
/// </summary>
public class KafkaConsumerBrokerFileConfig
{
    /// <summary>
    /// Comma-separated list of host:port broker addresses.
    /// Default: <c>localhost:9092</c>.
    /// </summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    /// <summary>
    /// Consumer group identifier. All consumers in the same group share
    /// partition assignment. Default: a new random GUID (no sharing).
    /// </summary>
    public string GroupId { get; set; } = Guid.NewGuid().ToString();

    /// <summary>Optional logical client name included in broker logs.</summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Automatically commit consumed offsets at the interval defined by
    /// AutoCommitIntervalMs. Default: <c>true</c>.
    /// </summary>
    public bool EnableAutoCommit { get; set; } = true;

    /// <summary>
    /// Frequency in milliseconds at which offsets are auto-committed.
    /// Only used when EnableAutoCommit is true. Default: <c>5000</c>.
    /// </summary>
    public int AutoCommitIntervalMs { get; set; } = 5000;

    /// <summary>
    /// Client group session keep-alive heartbeat interval in milliseconds.
    /// Should be ≤ SessionTimeoutMs / 3. Default: <c>3000</c>.
    /// </summary>
    public int HeartbeatIntervalMs { get; set; } = 3000;

    /// <summary>
    /// Timeout in milliseconds used to detect consumer failures when using
    /// the group management facility. Default: <c>30000</c>.
    /// </summary>
    public int SessionTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Maximum allowed time between calls to Consume before the consumer
    /// is considered failed and the group rebalances. Default: <c>300000</c>.
    /// </summary>
    public int MaxPollIntervalMs { get; set; } = 300000;

    /// <summary>
    /// Maximum data returned by the server for a fetch request (bytes).
    /// Default: <c>52428800</c> (50 MB).
    /// </summary>
    public int FetchMaxBytes { get; set; } = 52428800;

    /// <summary>
    /// Maximum data per partition per fetch request (bytes).
    /// Default: <c>1048576</c> (1 MB).
    /// </summary>
    public int MaxPartitionFetchBytes { get; set; } = 1048576;

    /// <summary>
    /// Minimum data the broker should accumulate before responding to a
    /// fetch request (bytes). Higher values improve throughput but add
    /// latency. Default: <c>1</c>.
    /// </summary>
    public int FetchMinBytes { get; set; } = 1;

    /// <summary>
    /// Maximum time in milliseconds the broker may wait before answering a
    /// fetch request to accumulate FetchMinBytes of data.
    /// Default: <c>500</c>.
    /// </summary>
    public int FetchWaitMaxMs { get; set; } = 500;

    /// <summary>
    /// How long in ms to cache the broker address before re-resolving DNS.
    /// Default: <c>1000</c>.
    /// </summary>
    public int BrokerAddressTtl { get; set; } = 1000;

    /// <summary>
    /// Minimum number of messages per partition that the client queues locally
    /// before fetching more. Default: <c>100000</c>.
    /// </summary>
    public int QueuedMinMessages { get; set; } = 100000;

    /// <summary>
    /// Maximum kilobytes of messages queued locally per partition.
    /// Default: <c>65536</c>.
    /// </summary>
    public int QueuedMaxMessagesKbytes { get; set; } = 65536;

    /// <summary>
    /// How long in ms to wait before retrying a failed fetch request.
    /// Default: <c>500</c>.
    /// </summary>
    public int FetchErrorBackoffMs { get; set; } = 500;

    /// <summary>
    /// Initial backoff duration in ms before reconnecting to a broker after a
    /// connection failure. Default: <c>100</c>.
    /// </summary>
    public int ReconnectBackoffMs { get; set; } = 100;

    /// <summary>
    /// Maximum backoff in ms between reconnection attempts (exponential
    /// back-off up to this cap). Default: <c>10000</c>.
    /// </summary>
    public int ReconnectBackoffMaxMs { get; set; } = 10000;

    /// <summary>
    /// Timeout in ms for socket reads. Any outstanding requests will fail
    /// after this time. Default: <c>60000</c>.
    /// </summary>
    public int SocketTimeoutMs { get; set; } = 60000;

    /// <summary>
    /// Enable TCP keepalives on broker sockets. Default: <c>false</c>.
    /// </summary>
    public bool SocketKeepaliveEnable { get; set; } = false;

    /// <summary>
    /// How often in ms the topic and broker metadata is refreshed in the
    /// background. Default: <c>300000</c>.
    /// </summary>
    public int TopicMetadataRefreshIntervalMs { get; set; } = 300000;

    /// <summary>
    /// Maximum age in ms of locally cached metadata before a forced refresh
    /// is triggered. Default: <c>900000</c>.
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
