// <copyright file="RouterMetricProviders.cs" company="Motion Applied Ltd.">
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

using Prometheus;

namespace MA.DataPlatforms.Secu4.Routing.PrometheusMetrics;

/// <summary>
/// Prometheus metrics describing the actual state of the SECU4 router's Kafka clients, sourced
/// entirely from the Confluent.Kafka / librdkafka statistics callback (no application-level
/// counters). The router has no queues or buffers of its own — the only real state is what the
/// Kafka client itself reports: internal queue depth/size, in-flight requests, broker round-trip
/// time, produce latency and consumer lag.
///
/// <para>
/// These are populated by <see cref="RouterStatisticsCollector"/>, which is wired to the producer
/// and consumer builders via <c>SetStatisticsHandler</c>. The callback only fires when
/// <c>statistics.interval.ms</c> (StatisticsIntervalMs) is &gt; 0.
/// </para>
/// </summary>
public static class RouterMetricProviders
{
    private const string ClientIdLabel = "client_id";
    private const string RoleLabel = "role";
    private const string BrokerLabel = "broker";
    private const string TopicLabel = "topic";
    private const string PartitionLabel = "partition";

    // ── Client-wide state {client_id, role} ───────────────────────────────────

    public static Gauge MessageCount { get; } = Metrics.CreateGauge(
        "router_librdkafka_msg_cnt",
        "Current number of messages in the Kafka client's internal queues (producer: not yet sent; consumer: fetched not yet read).",
        ClientIdLabel,
        RoleLabel);

    public static Gauge MessageSizeBytes { get; } = Metrics.CreateGauge(
        "router_librdkafka_msg_size_bytes",
        "Current total bytes of messages in the Kafka client's internal queues.",
        ClientIdLabel,
        RoleLabel);

    public static Gauge ReplyQueue { get; } = Metrics.CreateGauge(
        "router_librdkafka_replyq",
        "Number of operations waiting in librdkafka's main reply queue.",
        ClientIdLabel,
        RoleLabel);

    public static Gauge TransmittedMessages { get; } = Metrics.CreateGauge(
        "router_librdkafka_txmsgs_total",
        "Total messages transmitted to brokers (cumulative, reported by librdkafka).",
        ClientIdLabel,
        RoleLabel);

    public static Gauge ReceivedMessages { get; } = Metrics.CreateGauge(
        "router_librdkafka_rxmsgs_total",
        "Total messages received from brokers (cumulative, reported by librdkafka).",
        ClientIdLabel,
        RoleLabel);

    public static Gauge TransmittedBytes { get; } = Metrics.CreateGauge(
        "router_librdkafka_tx_bytes_total",
        "Total message bytes transmitted to brokers (cumulative, reported by librdkafka).",
        ClientIdLabel,
        RoleLabel);

    public static Gauge ReceivedBytes { get; } = Metrics.CreateGauge(
        "router_librdkafka_rx_bytes_total",
        "Total message bytes received from brokers (cumulative, reported by librdkafka).",
        ClientIdLabel,
        RoleLabel);

    // ── Per-broker state {client_id, broker} ──────────────────────────────────

    public static Gauge BrokerOutboundMessages { get; } = Metrics.CreateGauge(
        "router_librdkafka_broker_outbuf_msgs",
        "Messages waiting in the broker's outbound queue (not yet sent).",
        ClientIdLabel,
        BrokerLabel);

    public static Gauge BrokerWaitingResponseMessages { get; } = Metrics.CreateGauge(
        "router_librdkafka_broker_waitresp_msgs",
        "Messages in-flight to the broker awaiting a response.",
        ClientIdLabel,
        BrokerLabel);

    public static Gauge BrokerRoundTripTimeAvgMicroseconds { get; } = Metrics.CreateGauge(
        "router_librdkafka_broker_rtt_avg_microseconds",
        "Average broker round-trip time over the last stats window (microseconds).",
        ClientIdLabel,
        BrokerLabel);

    public static Gauge BrokerInternalLatencyAvgMicroseconds { get; } = Metrics.CreateGauge(
        "router_librdkafka_broker_int_latency_avg_microseconds",
        "Average time a produced message spent in the internal queue before transmission (microseconds).",
        ClientIdLabel,
        BrokerLabel);

    // ── Per-partition state {client_id, topic, partition} ─────────────────────

    public static Gauge PartitionMessageQueue { get; } = Metrics.CreateGauge(
        "router_librdkafka_partition_msgq_cnt",
        "Messages awaiting production in the partition's local queue.",
        ClientIdLabel,
        TopicLabel,
        PartitionLabel);

    public static Gauge PartitionTransmitMessageQueue { get; } = Metrics.CreateGauge(
        "router_librdkafka_partition_xmit_msgq_cnt",
        "Messages ready for transmission in the partition's transmit queue.",
        ClientIdLabel,
        TopicLabel,
        PartitionLabel);

    public static Gauge ConsumerLag { get; } = Metrics.CreateGauge(
        "router_consumer_lag",
        "Consumer lag (next offset to fetch minus last consumed offset) per topic/partition, reported by librdkafka.",
        ClientIdLabel,
        TopicLabel,
        PartitionLabel);
}
