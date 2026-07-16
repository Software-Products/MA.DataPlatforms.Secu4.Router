// <copyright file="RouterStatisticsCollectorShould.cs" company="Motion Applied Ltd.">
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

using FluentAssertions;

using MA.DataPlatforms.Secu4.Routing.PrometheusMetrics;

using Xunit;

namespace MA.DataPlatforms.Secu4.Routing.PrometheusMetrics.UnitTest;

public class RouterStatisticsCollectorShould
{
    // The metrics live in the global Prometheus registry, so each test uses a unique client_id
    // to avoid cross-test interference on shared gauges.
    private readonly string clientId = $"client-{Guid.NewGuid():N}";

    [Fact]
    public void Populate_Client_Wide_Gauges_For_A_Producer()
    {
        var json = $$"""
        {
            "client_id": "{{this.clientId}}",
            "type": "producer",
            "replyq": 3,
            "msg_cnt": 42,
            "msg_size": 1024,
            "txmsgs": 100,
            "rxmsgs": 7,
            "txmsg_bytes": 200000,
            "rxmsg_bytes": 64
        }
        """;

        RouterStatisticsCollector.Handle(json);

        RouterMetricProviders.MessageCount.WithLabels(this.clientId, "producer").Value.Should().Be(42);
        RouterMetricProviders.MessageSizeBytes.WithLabels(this.clientId, "producer").Value.Should().Be(1024);
        RouterMetricProviders.ReplyQueue.WithLabels(this.clientId, "producer").Value.Should().Be(3);
        RouterMetricProviders.TransmittedMessages.WithLabels(this.clientId, "producer").Value.Should().Be(100);
        RouterMetricProviders.ReceivedMessages.WithLabels(this.clientId, "producer").Value.Should().Be(7);
        RouterMetricProviders.TransmittedBytes.WithLabels(this.clientId, "producer").Value.Should().Be(200000);
        RouterMetricProviders.ReceivedBytes.WithLabels(this.clientId, "producer").Value.Should().Be(64);
    }

    [Fact]
    public void Populate_Per_Broker_Gauges_And_Skip_Unconnected_Brokers()
    {
        var json = $$"""
        {
            "client_id": "{{this.clientId}}",
            "type": "producer",
            "brokers": {
                "kafka:9092/1": {
                    "name": "kafka:9092/1",
                    "nodeid": 1,
                    "outbuf_msg_cnt": 5,
                    "waitresp_msg_cnt": 2,
                    "rtt": { "min": 100, "avg": 750, "p99": 1200 },
                    "int_latency": { "min": 10, "avg": 333, "p99": 900 }
                },
                "GroupCoordinator": {
                    "name": "GroupCoordinator",
                    "nodeid": -1,
                    "outbuf_msg_cnt": 99
                }
            }
        }
        """;

        RouterStatisticsCollector.Handle(json);

        RouterMetricProviders.BrokerOutboundMessages.WithLabels(this.clientId, "kafka:9092/1").Value.Should().Be(5);
        RouterMetricProviders.BrokerWaitingResponseMessages.WithLabels(this.clientId, "kafka:9092/1").Value.Should().Be(2);
        RouterMetricProviders.BrokerRoundTripTimeAvgMicroseconds.WithLabels(this.clientId, "kafka:9092/1").Value.Should().Be(750);
        RouterMetricProviders.BrokerInternalLatencyAvgMicroseconds.WithLabels(this.clientId, "kafka:9092/1").Value.Should().Be(333);

        // The unconnected broker (nodeid -1) must not have produced a gauge value.
        RouterMetricProviders.BrokerOutboundMessages.WithLabels(this.clientId, "GroupCoordinator").Value.Should().Be(0);
    }

    [Fact]
    public void Populate_Per_Partition_Queue_Gauges_And_Skip_The_Internal_Aggregate_Partition()
    {
        var json = $$"""
        {
            "client_id": "{{this.clientId}}",
            "type": "producer",
            "topics": {
                "my-topic": {
                    "partitions": {
                        "0": { "msgq_cnt": 11, "xmit_msgq_cnt": 4 },
                        "-1": { "msgq_cnt": 999, "xmit_msgq_cnt": 999 }
                    }
                }
            }
        }
        """;

        RouterStatisticsCollector.Handle(json);

        RouterMetricProviders.PartitionMessageQueue.WithLabels(this.clientId, "my-topic", "0").Value.Should().Be(11);
        RouterMetricProviders.PartitionTransmitMessageQueue.WithLabels(this.clientId, "my-topic", "0").Value.Should().Be(4);

        // The internal "-1" aggregate partition must be ignored.
        RouterMetricProviders.PartitionMessageQueue.WithLabels(this.clientId, "my-topic", "-1").Value.Should().Be(0);
    }

    [Fact]
    public void Populate_Consumer_Lag_For_A_Consumer_Skipping_Internal_And_Unavailable_Lag()
    {
        var json = $$"""
        {
            "client_id": "{{this.clientId}}",
            "type": "consumer",
            "topics": {
                "my-topic": {
                    "partitions": {
                        "0": { "consumer_lag": 25 },
                        "1": { "consumer_lag": -1 },
                        "-1": { "consumer_lag": 12345 }
                    }
                }
            }
        }
        """;

        RouterStatisticsCollector.Handle(json);

        RouterMetricProviders.ConsumerLag.WithLabels(this.clientId, "my-topic", "0").Value.Should().Be(25);

        // -1 lag means "not available yet" and partition "-1" is the internal aggregate; neither is recorded.
        RouterMetricProviders.ConsumerLag.WithLabels(this.clientId, "my-topic", "1").Value.Should().Be(0);
        RouterMetricProviders.ConsumerLag.WithLabels(this.clientId, "my-topic", "-1").Value.Should().Be(0);
    }

    [Fact]
    public void Not_Populate_Consumer_Lag_For_A_Producer()
    {
        var json = $$"""
        {
            "client_id": "{{this.clientId}}",
            "type": "producer",
            "topics": {
                "my-topic": {
                    "partitions": {
                        "0": { "consumer_lag": 25 }
                    }
                }
            }
        }
        """;

        RouterStatisticsCollector.Handle(json);

        RouterMetricProviders.ConsumerLag.WithLabels(this.clientId, "my-topic", "0").Value.Should().Be(0);
    }

    [Fact]
    public void Fall_Back_To_Name_When_Client_Id_Is_Missing()
    {
        var name = $"name-{Guid.NewGuid():N}";
        var json = $$"""
        {
            "name": "{{name}}",
            "type": "producer",
            "msg_cnt": 9
        }
        """;

        RouterStatisticsCollector.Handle(json);

        RouterMetricProviders.MessageCount.WithLabels(name, "producer").Value.Should().Be(9);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    [InlineData("{ not valid json ")]
    [InlineData("[]")]
    public void Not_Throw_On_Empty_Or_Malformed_Input(string? input)
    {
        var act = () => RouterStatisticsCollector.Handle(input!);

        act.Should().NotThrow();
    }

    [Fact]
    public void Treat_Missing_Numeric_Fields_As_Zero()
    {
        var json = $$"""
        {
            "client_id": "{{this.clientId}}",
            "type": "producer"
        }
        """;

        RouterStatisticsCollector.Handle(json);

        RouterMetricProviders.MessageCount.WithLabels(this.clientId, "producer").Value.Should().Be(0);
        RouterMetricProviders.MessageSizeBytes.WithLabels(this.clientId, "producer").Value.Should().Be(0);
    }
}
