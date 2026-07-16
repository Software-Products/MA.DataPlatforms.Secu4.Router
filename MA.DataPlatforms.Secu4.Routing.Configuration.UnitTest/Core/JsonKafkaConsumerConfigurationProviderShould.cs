// <copyright file="JsonKafkaConsumerConfigurationProviderShould.cs" company="Motion Applied Ltd.">
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

public class JsonKafkaConsumerConfigurationProviderShould
{
    [Fact]
    public void Map_Routes_And_Broker_Settings()
    {
        const string JSON = """
        {
          "design": {
            "routes": [
              { "name": "r1", "topic": "topic-a", "partition": 2, "autoOffsetReset": "Earliest" }
            ],
            "topicsMetaData": [
              { "topic": "topic-a", "numberOfPartitions": 6, "replicationFactor": 3 }
            ]
          },
          "kafkaBroker": {
            "bootstrapServers": "broker:9092",
            "groupId": "group-1",
            "clientId": "client-1",
            "fetchMaxBytes": 12345,
            "maxPartitionFetchBytes": 678
          }
        }
        """;

        using var file = new TempJsonFile(JSON);
        var provider = new JsonKafkaConsumerConfigurationProvider(file.Path);

        var result = provider.Provide();

        result.KafkaConsumingConfigs.Should().HaveCount(1);
        var config = result.KafkaConsumingConfigs[0];
        config.KafkaRoutes.Name.Should().Be("r1");
        config.KafkaRoutes.Topic.Should().Be("topic-a");
        config.KafkaRoutes.Partition.Should().Be(2);
        config.KafkaListeningConfig.Server.Should().Be("broker:9092");
        config.KafkaListeningConfig.GroupId.Should().Be("group-1");
        config.KafkaListeningConfig.Properties["client.id"].Should().Be("client-1");
        config.KafkaListeningConfig.Properties["fetch.max.bytes"].Should().Be("12345");
        config.KafkaListeningConfig.Properties["max.partition.fetch.bytes"].Should().Be("678");
        config.KafkaListeningConfig.AutoOffsetResetMode.Should().Be(AutoOffsetResetMode.Earliest);
        config.RoutesMetaData.Topic.Should().Be("topic-a");
        config.RoutesMetaData.NumberOfPartitions.Should().Be(6);
        config.RoutesMetaData.ReplicationFactor.Should().Be((short)3);
        config.Security.Should().BeNull();
    }

    [Theory]
    [InlineData("Earliest", AutoOffsetResetMode.Earliest)]
    [InlineData("earliest", AutoOffsetResetMode.Earliest)]
    [InlineData("FromOffset", AutoOffsetResetMode.FromOffset)]
    [InlineData("from_offset", AutoOffsetResetMode.FromOffset)]
    [InlineData("Latest", AutoOffsetResetMode.Latest)]
    [InlineData("anything-else", AutoOffsetResetMode.Latest)]
    public void Map_AutoOffsetReset_Modes(string raw, AutoOffsetResetMode expected)
    {
        var json = $$"""
        {
          "design": {
            "routes": [ { "name": "r", "topic": "t", "autoOffsetReset": "{{raw}}", "fromOffset": 42 } ]
          },
          "kafkaBroker": { "bootstrapServers": "b:9092" }
        }
        """;

        using var file = new TempJsonFile(json);
        var result = new JsonKafkaConsumerConfigurationProvider(file.Path).Provide();

        var listening = result.KafkaConsumingConfigs[0].KafkaListeningConfig;
        listening.AutoOffsetResetMode.Should().Be(expected);
        listening.Offset.Should().Be(42);
    }

    [Fact]
    public void Use_Default_Topic_Metadata_When_Not_Declared()
    {
        const string JSON = """
        {
          "design": { "routes": [ { "name": "r", "topic": "lonely-topic" } ] },
          "kafkaBroker": { "bootstrapServers": "b:9092" }
        }
        """;

        using var file = new TempJsonFile(JSON);
        var result = new JsonKafkaConsumerConfigurationProvider(file.Path).Provide();

        var metadata = result.KafkaConsumingConfigs[0].RoutesMetaData;
        metadata.Topic.Should().Be("lonely-topic");
        metadata.NumberOfPartitions.Should().BeNull();
        metadata.ReplicationFactor.Should().BeNull();
    }

    [Theory]
    [InlineData("Ssl", KafkaSecurityProtocol.Ssl)]
    [InlineData("SaslPlaintext", KafkaSecurityProtocol.SaslPlaintext)]
    [InlineData("sasl_plaintext", KafkaSecurityProtocol.SaslPlaintext)]
    [InlineData("SaslSsl", KafkaSecurityProtocol.SaslSsl)]
    [InlineData("sasl_ssl", KafkaSecurityProtocol.SaslSsl)] 
    public void Map_Security_Protocols(string raw, KafkaSecurityProtocol expected)
    {
        var json = $$"""
        {
          "design": { "routes": [ { "name": "r", "topic": "t" } ] },
          "kafkaBroker": {
            "bootstrapServers": "b:9092",
            "security": { "protocol": "{{raw}}", "saslUsername": "u" }
          }
        }
        """;

        using var file = new TempJsonFile(json);
        var result = new JsonKafkaConsumerConfigurationProvider(file.Path).Provide();

        result.KafkaConsumingConfigs[0].Security!.Protocol.Should().Be(expected);
    }

    [Theory]
    [InlineData("ScramSha256", KafkaSaslMechanism.ScramSha256)]
    [InlineData("scram-sha-256", KafkaSaslMechanism.ScramSha256)]
    [InlineData("ScramSha512", KafkaSaslMechanism.ScramSha512)]
    [InlineData("scram-sha-512", KafkaSaslMechanism.ScramSha512)]
    [InlineData("Gssapi", KafkaSaslMechanism.Gssapi)]
    [InlineData("OAuthBearer", KafkaSaslMechanism.OAuthBearer)]
    [InlineData("Plain", KafkaSaslMechanism.Plain)]
    public void Map_Sasl_Mechanisms(string raw, KafkaSaslMechanism expected)
    {
        var json = $$"""
        {
          "design": { "routes": [ { "name": "r", "topic": "t" } ] },
          "kafkaBroker": {
            "bootstrapServers": "b:9092",
            "security": { "protocol": "SaslSsl", "saslMechanism": "{{raw}}" }
          }
        }
        """;

        using var file = new TempJsonFile(json);
        var result = new JsonKafkaConsumerConfigurationProvider(file.Path).Provide();

        result.KafkaConsumingConfigs[0].Security!.SaslMechanism.Should().Be(expected);
    }

    [Fact]
    public void Resolve_Secrets_Through_Resolver()
    {
        const string JSON = """
        {
          "design": { "routes": [ { "name": "r", "topic": "t" } ] },
          "kafkaBroker": {
            "bootstrapServers": "b:9092",
            "security": {
              "protocol": "SaslSsl",
              "saslUsername": "${USER}",
              "saslPassword": "${PASS}",
              "sslKeyLocation": "${KEY}",
              "sslKeyPassword": "${KEYPASS}",
              "sslCaLocation": "/ca.pem",
              "sslCertificateLocation": "/cert.pem"
            }
          }
        }
        """;

        var resolver = Substitute.For<ISecretResolver>();
        resolver.Resolve("${USER}").Returns("real-user");
        resolver.Resolve("${PASS}").Returns("real-pass");
        resolver.Resolve("${KEY}").Returns("/real/key");
        resolver.Resolve("${KEYPASS}").Returns("real-key-pass");

        using var file = new TempJsonFile(JSON);
        var result = new JsonKafkaConsumerConfigurationProvider(file.Path, resolver).Provide();

        var security = result.KafkaConsumingConfigs[0].Security!;
        security.SaslUsername.Should().Be("real-user");
        security.SaslPassword.Should().Be("real-pass");
        security.SslKeyLocation.Should().Be("/real/key");
        security.SslKeyPassword.Should().Be("real-key-pass");
        security.SslCaLocation.Should().Be("/ca.pem");
        security.SslCertificateLocation.Should().Be("/cert.pem");
    }

    [Fact]
    public void Prefer_Security_Override_Provider_Over_File()
    {
        const string JSON = """
        {
          "design": { "routes": [ { "name": "r", "topic": "t" } ] },
          "kafkaBroker": {
            "bootstrapServers": "b:9092",
            "security": { "protocol": "SaslSsl", "saslUsername": "file-user" }
          }
        }
        """;

        var overrideSecurity = new KafkaSecurityConfig(KafkaSecurityProtocol.Ssl, saslUsername: "override-user");
        var securityProvider = Substitute.For<IKafkaSecurityConfigProvider>();
        securityProvider.Provide().Returns(overrideSecurity);

        using var file = new TempJsonFile(JSON);
        var result = new JsonKafkaConsumerConfigurationProvider(file.Path, securityConfigProvider: securityProvider).Provide();

        result.KafkaConsumingConfigs[0].Security.Should().BeSameAs(overrideSecurity);
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
        var provider = new JsonKafkaConsumerConfigurationProvider(file.Path);

        var first = provider.Provide();
        var second = provider.Provide();

        second.Should().BeSameAs(first);
    }

    [Fact]
    public void Throw_When_Json_Deserialises_To_Null()
    {
        using var file = new TempJsonFile("null");
        var provider = new JsonKafkaConsumerConfigurationProvider(file.Path);

        var act = () => provider.Provide();

        act.Should().Throw<InvalidOperationException>();
    }
}
