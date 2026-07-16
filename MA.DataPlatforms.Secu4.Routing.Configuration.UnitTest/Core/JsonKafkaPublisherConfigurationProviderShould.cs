// <copyright file="JsonKafkaPublisherConfigurationProviderShould.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Configuration.Core;
using MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Helpers;
using MA.DataPlatforms.Secu4.Routing.Contracts;

using NSubstitute;

using Xunit;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Core;

public class JsonKafkaPublisherConfigurationProviderShould
{
    [Fact]
    public void Map_Routes_Metadata_And_Broker_Settings()
    {
        const string JSON = """
        {
          "design": {
            "deadLetterTopic": "dlq",
            "routes": [
              { "name": "r1", "topic": "topic-a", "partition": 1 },
              { "name": "r2", "topic": "topic-b" }
            ],
            "topicsMetaData": [
              { "topic": "topic-a", "numberOfPartitions": 3, "replicationFactor": 2 }
            ]
          },
          "kafkaBroker": {
            "bootstrapServers": "broker:9092",
            "clientId": "pub-client",
            "acks": "1",
            "lingerMs": 50,
            "batchSize": 99999
          }
        }
        """;

        using var file = new TempJsonFile(JSON);
        var routing = new JsonKafkaPublisherConfigurationProvider(file.Path).Provide().KafkaRoutingConfig;

        routing.DeadLetterTopic.Should().Be("dlq");
        routing.KafkaRoutes.Should().HaveCount(2);
        routing.KafkaRoutes[0].Name.Should().Be("r1");
        routing.KafkaRoutes[0].Partition.Should().Be(1);
        routing.RoutesMetaData.Should().ContainSingle();
        routing.RoutesMetaData[0].NumberOfPartitions.Should().Be(3);
        routing.KafkaPublishingConfig.Server.Should().Be("broker:9092");
        routing.KafkaPublishingConfig.Properties["client.id"].Should().Be("pub-client");
        routing.KafkaPublishingConfig.Properties["acks"].Should().Be("1");
        routing.KafkaPublishingConfig.Properties["linger.ms"].Should().Be("50");
        routing.KafkaPublishingConfig.Properties["batch.size"].Should().Be("99999");
        routing.Security.Should().BeNull();
    }

    [Theory]
    [InlineData("gzip")]
    [InlineData("snappy")]
    [InlineData("lz4")]
    [InlineData("zstd")]
    [InlineData("none")]
    [InlineData("unknown")]
    public void Map_Compression_Types(string raw)
    {
        var json = $$"""
        {
          "design": { "routes": [ { "name": "r", "topic": "t" } ] },
          "kafkaBroker": { "bootstrapServers": "b:9092", "compressionType": "{{raw}}" }
        }
        """;

        using var file = new TempJsonFile(json);
        var routing = new JsonKafkaPublisherConfigurationProvider(file.Path).Provide().KafkaRoutingConfig;

        routing.KafkaPublishingConfig.Properties["compression.type"].Should().Be(raw);
    }

    [Fact]
    public void Map_Security_With_Resolved_Secrets()
    {
        const string JSON = """
        {
          "design": { "routes": [ { "name": "r", "topic": "t" } ] },
          "kafkaBroker": {
            "bootstrapServers": "b:9092",
            "security": { "protocol": "SaslSsl", "saslMechanism": "ScramSha512", "saslPassword": "${PASS}" }
          }
        }
        """;

        var resolver = Substitute.For<ISecretResolver>();
        resolver.Resolve("${PASS}").Returns("secret");

        using var file = new TempJsonFile(JSON);
        var routing = new JsonKafkaPublisherConfigurationProvider(file.Path, resolver).Provide().KafkaRoutingConfig;

        routing.Security!.Protocol.Should().Be(KafkaSecurityProtocol.SaslSsl);
        routing.Security.SaslMechanism.Should().Be(KafkaSaslMechanism.ScramSha512);
        routing.Security.SaslPassword.Should().Be("secret");
    }

    [Fact]
    public void Prefer_Security_Override_Provider_Over_File()
    {
        const string JSON = """
        {
          "design": { "routes": [ { "name": "r", "topic": "t" } ] },
          "kafkaBroker": {
            "bootstrapServers": "b:9092",
            "security": { "protocol": "SaslSsl" }
          }
        }
        """;

        var overrideSecurity = new KafkaSecurityConfig(KafkaSecurityProtocol.Ssl);
        var securityProvider = Substitute.For<IKafkaSecurityConfigProvider>();
        securityProvider.Provide().Returns(overrideSecurity);

        using var file = new TempJsonFile(JSON);
        var routing = new JsonKafkaPublisherConfigurationProvider(file.Path, securityConfigProvider: securityProvider)
            .Provide().KafkaRoutingConfig;

        routing.Security.Should().BeSameAs(overrideSecurity);
    }

    [Fact]
    public void Cache_Result_Between_Calls()
    {
        const string JSON = """
        {
          "design": { "routes": [ { "name": "r", "topic": "t" } ] },
          "kafkaBroker": { "bootstrapServers": "b:9092" }
        }
        """;

        using var file = new TempJsonFile(JSON);
        var provider = new JsonKafkaPublisherConfigurationProvider(file.Path);

        provider.Provide().Should().BeSameAs(provider.Provide());
    }

    [Fact]
    public void Throw_When_Json_Deserialises_To_Null()
    {
        using var file = new TempJsonFile("null");
        var provider = new JsonKafkaPublisherConfigurationProvider(file.Path);

        var act = () => provider.Provide();

        act.Should().Throw<InvalidOperationException>();
    }
}
