// <copyright file="RouterStatisticsCollector.cs" company="Motion Applied Ltd.">
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

using System.Text.Json;

namespace MA.DataPlatforms.Secu4.Routing.PrometheusMetrics;

/// <summary>
/// Parses the librdkafka statistics JSON emitted by the Confluent.Kafka statistics callback and
/// updates the <see cref="RouterMetricProviders"/> gauges. This is the router's only real client
/// state — internal queue depth/size, in-flight totals, broker round-trip time and consumer lag.
///
/// <para>
/// Wire it from the producer/consumer builder:
/// <code>
/// builder.SetStatisticsHandler((_, json) =&gt; RouterStatisticsCollector.Handle(json));
/// </code>
/// The callback only fires when <c>statistics.interval.ms</c> (StatisticsIntervalMs) is &gt; 0, so
/// set it (e.g. 5000) in the publisher/consumer broker config for these gauges to populate.
/// </para>
/// </summary>
public static class RouterStatisticsCollector
{
    /// <summary>
    /// Parses one librdkafka statistics JSON document and updates the gauges. Never throws —
    /// statistics are best-effort diagnostics and must not disrupt produce/consume.
    /// </summary>
    public static void Handle(string statisticsJson)
    {
        if (string.IsNullOrWhiteSpace(statisticsJson))
        {
            return;
        }

        try
        {
            using var doc = JsonDocument.Parse(statisticsJson);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return;
            }

            var clientId = GetString(root, "client_id") ?? GetString(root, "name") ?? "unknown";
            var role = GetString(root, "type") ?? "unknown"; // "producer" or "consumer"

            RouterMetricProviders.MessageCount.WithLabels(clientId, role).Set(GetDouble(root, "msg_cnt"));
            RouterMetricProviders.MessageSizeBytes.WithLabels(clientId, role).Set(GetDouble(root, "msg_size"));
            RouterMetricProviders.ReplyQueue.WithLabels(clientId, role).Set(GetDouble(root, "replyq"));
            RouterMetricProviders.TransmittedMessages.WithLabels(clientId, role).Set(GetDouble(root, "txmsgs"));
            RouterMetricProviders.ReceivedMessages.WithLabels(clientId, role).Set(GetDouble(root, "rxmsgs"));
            RouterMetricProviders.TransmittedBytes.WithLabels(clientId, role).Set(GetDouble(root, "txmsg_bytes"));
            RouterMetricProviders.ReceivedBytes.WithLabels(clientId, role).Set(GetDouble(root, "rxmsg_bytes"));

            UpdateBrokers(root, clientId);
            UpdateTopicsAndPartitions(root, clientId, role);
        }
        catch (JsonException)
        {
            // Malformed/partial stats payload — ignore this tick.
        }
    }

    private static void UpdateBrokers(JsonElement root, string clientId)
    {
        if (!root.TryGetProperty("brokers", out var brokers) || brokers.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        foreach (var broker in brokers.EnumerateObject())
        {
            // Skip the bootstrap/learned broker entries that never connected (nodeid -1).
            if (GetDouble(broker.Value, "nodeid") < 0)
            {
                continue;
            }

            var brokerName = GetString(broker.Value, "name") ?? broker.Name;

            RouterMetricProviders.BrokerOutboundMessages.WithLabels(clientId, brokerName).Set(GetDouble(broker.Value, "outbuf_msg_cnt"));
            RouterMetricProviders.BrokerWaitingResponseMessages.WithLabels(clientId, brokerName).Set(GetDouble(broker.Value, "waitresp_msg_cnt"));
            RouterMetricProviders.BrokerRoundTripTimeAvgMicroseconds.WithLabels(clientId, brokerName).Set(GetNestedAvg(broker.Value, "rtt"));
            RouterMetricProviders.BrokerInternalLatencyAvgMicroseconds.WithLabels(clientId, brokerName).Set(GetNestedAvg(broker.Value, "int_latency"));
        }
    }

    private static void UpdateTopicsAndPartitions(JsonElement root, string clientId, string role)
    {
        if (!root.TryGetProperty("topics", out var topics) || topics.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        foreach (var topic in topics.EnumerateObject())
        {
            if (!topic.Value.TryGetProperty("partitions", out var partitions) || partitions.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            UpdatePartitions(clientId, role, topic.Name, partitions);
        }
    }

    private static void UpdatePartitions(string clientId, string role, string topicName, JsonElement partitions)
    {
        foreach (var partition in partitions.EnumerateObject())
        {
            // librdkafka includes an internal partition "-1" aggregate — skip it.
            if (partition.Name == "-1")
            {
                continue;
            }

            RouterMetricProviders.PartitionMessageQueue.WithLabels(clientId, topicName, partition.Name).Set(GetDouble(partition.Value, "msgq_cnt"));
            RouterMetricProviders.PartitionTransmitMessageQueue.WithLabels(clientId, topicName, partition.Name).Set(GetDouble(partition.Value, "xmit_msgq_cnt"));

            if (role != "consumer")
            {
                continue;
            }

            var lag = GetDouble(partition.Value, "consumer_lag");
            if (lag >= 0)
            {
                RouterMetricProviders.ConsumerLag.WithLabels(clientId, topicName, partition.Name).Set(lag);
            }
        }
    }

    private static string? GetString(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    private static double GetDouble(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.Number
            ? value.GetDouble()
            : 0d;

    // librdkafka reports latencies as a window object: { "min":.., "avg":.., "p99":.., ... }.
    private static double GetNestedAvg(JsonElement element, string property) =>
        element.TryGetProperty(property, out var window) && window.ValueKind == JsonValueKind.Object
            ? GetDouble(window, "avg")
            : 0d;
}
