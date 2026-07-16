// <copyright file="JsonKafkaPublisherBrokerOverrideProviderShould.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;
using MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Helpers;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;

using NSubstitute;

using Xunit;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Core;

public class JsonKafkaPublisherBrokerOverrideProviderShould
{
    private static IRoutingConfigurationProvider CreateInner(KafkaSecurityConfig? security = null)
    {
        var publishing = new KafkaPublishingConfig(
            server: "inner:9092",
            properties: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["compression.type"]             = "snappy",
                ["linger.ms"]                    = "33",
                ["queue.buffering.max.messages"]  = "10",
                ["queue.buffering.max.kbytes"]    = "20",
                ["client.id"]                    = "inner-client",
                ["acks"]                         = "1",
            });

        var routingConfig = new KafkaRoutingConfig(
            publishing,
            [new KafkaRoute("inner-route", "inner-topic", 1)],
            [new KafkaTopicMetaData("inner-topic", 3, 1)],
            "inner-dlq",
            security);

        var inner = Substitute.For<IRoutingConfigurationProvider>();
        inner.Provide().Returns(new RoutingConfiguration(routingConfig));
        return inner;
    }

    [Fact]
    public void Override_Broker_Fields_But_Keep_Design()
    {
        // bootstrap.servers is always excluded — server stays from the inner provider.
        const string BrokerJson = """
        {
          "client.id": "file-client",
          "compression.type": "lz4",
          "linger.ms": 99,
          "acks": "all"
        }
        """;

        using var file = new TempJsonFile(BrokerJson);
        var routing = new JsonKafkaPublisherBrokerOverrideProvider(CreateInner(), file.Path)
            .Provide().KafkaRoutingConfig;

        routing.KafkaPublishingConfig.Server.Should().Be("inner:9092");
        routing.KafkaPublishingConfig.Properties["client.id"].Should().Be("file-client");
        routing.KafkaPublishingConfig.Properties["compression.type"].Should().Be("lz4");
        routing.KafkaPublishingConfig.Properties["linger.ms"].Should().Be("99");
        routing.KafkaPublishingConfig.Properties["acks"].Should().Be("all");

        // Design preserved.
        routing.DeadLetterTopic.Should().Be("inner-dlq");
        routing.KafkaRoutes.Should().ContainSingle().Which.Name.Should().Be("inner-route");
        routing.RoutesMetaData.Should().ContainSingle().Which.Topic.Should().Be("inner-topic");
    }

    [Fact]
    public void Only_Override_Fields_Present_In_The_File_And_Keep_The_Rest_From_Inner()
    {
        // The file sets a single field — everything else must keep the inner (baseline) value,
        // NOT the file model's defaults (e.g. acks must stay "1", not become "all").
        const string BrokerJson = """
        {
          "linger.ms": 99
        }
        """;

        using var file = new TempJsonFile(BrokerJson);
        var routing = new JsonKafkaPublisherBrokerOverrideProvider(CreateInner(), file.Path)
            .Provide().KafkaRoutingConfig;

        routing.KafkaPublishingConfig.Properties["linger.ms"].Should().Be("99");           // overridden (present)
        routing.KafkaPublishingConfig.Server.Should().Be("inner:9092");                    // design field — never changes
        routing.KafkaPublishingConfig.Properties["compression.type"].Should().Be("snappy"); // absent -> kept from inner
        routing.KafkaPublishingConfig.Properties["client.id"].Should().Be("inner-client"); // absent -> kept from inner
        routing.KafkaPublishingConfig.Properties["queue.buffering.max.messages"].Should().Be("10"); // absent -> kept
        routing.KafkaPublishingConfig.Properties["acks"].Should().Be("1");                 // absent -> kept (not file default)
    }

    [Fact]
    public void Keep_Excluded_Fields_From_Inner_Provider()
    {
        const string BrokerJson = """
        {
          "client.id": "file-client",
          "compression.type": "zstd"
        }
        """;

        using var file = new TempJsonFile(BrokerJson);
        var routing = new JsonKafkaPublisherBrokerOverrideProvider(
                CreateInner(),
                file.Path,
                excludeFields: PublisherBrokerFields.ClientId
                             | PublisherBrokerFields.CompressionType)
            .Provide().KafkaRoutingConfig;

        routing.KafkaPublishingConfig.Properties["client.id"].Should().Be("inner-client");
        routing.KafkaPublishingConfig.Properties["compression.type"].Should().Be("snappy");
    }

    [Fact]
    public void Map_Security_From_File_By_Default()
    {
        const string BrokerJson = """
        {
          "linger.ms": 0,
          "security": { "protocol": "Ssl" }
        }
        """;

        using var file = new TempJsonFile(BrokerJson);
        var routing = new JsonKafkaPublisherBrokerOverrideProvider(CreateInner(), file.Path)
            .Provide().KafkaRoutingConfig;

        routing.Security!.Protocol.Should().Be(KafkaSecurityProtocol.Ssl);
    }

    [Fact]
    public void Keep_Inner_Security_When_Security_Field_Excluded()
    {
        const string BrokerJson = """
        {
          "linger.ms": 0,
          "security": { "protocol": "Ssl" }
        }
        """;

        var innerSecurity = new KafkaSecurityConfig(KafkaSecurityProtocol.SaslSsl);

        using var file = new TempJsonFile(BrokerJson);
        var routing = new JsonKafkaPublisherBrokerOverrideProvider(
                CreateInner(innerSecurity),
                file.Path,
                excludeFields: PublisherBrokerFields.Security)
            .Provide().KafkaRoutingConfig;

        routing.Security.Should().BeSameAs(innerSecurity);
    }

    [Fact]
    public void Prefer_Security_Override_Provider()
    {
        const string BrokerJson = """
        {
          "linger.ms": 0,
          "security": { "protocol": "Ssl" }
        }
        """;

        var overrideSecurity = new KafkaSecurityConfig(KafkaSecurityProtocol.SaslPlaintext);
        var securityProvider = Substitute.For<IKafkaSecurityConfigProvider>();
        securityProvider.Provide().Returns(overrideSecurity);

        using var file = new TempJsonFile(BrokerJson);
        var routing = new JsonKafkaPublisherBrokerOverrideProvider(
                CreateInner(),
                file.Path,
                securityConfigProvider: securityProvider)
            .Provide().KafkaRoutingConfig;

        routing.Security.Should().BeSameAs(overrideSecurity);
    }

    [Theory]
    [InlineData("gzip", "gzip")]
    [InlineData("snappy", "snappy")]
    [InlineData("lz4", "lz4")]
    [InlineData("zstd", "zstd")]
    [InlineData("none", "none")]
    public void Map_Compression_Types_From_File(string raw, string expected)
    {
        var brokerJson = $$"""{ "compression.type": "{{raw}}" }""";

        using var file = new TempJsonFile(brokerJson);
        var routing = new JsonKafkaPublisherBrokerOverrideProvider(CreateInner(), file.Path)
            .Provide().KafkaRoutingConfig;

        routing.KafkaPublishingConfig.Properties["compression.type"].Should().Be(expected);
    }

    [Fact]
    public void Keep_All_Broker_Fields_From_Inner_When_Excluding_All()
    {
        const string BrokerJson = """
        {
          "client.id": "file-client",
          "compression.type": "lz4",
          "linger.ms": 99,
          "acks": "all",
          "security": { "protocol": "Ssl" }
        }
        """;

        var innerSecurity = new KafkaSecurityConfig(KafkaSecurityProtocol.SaslSsl);

        using var file = new TempJsonFile(BrokerJson);
        var routing = new JsonKafkaPublisherBrokerOverrideProvider(
                CreateInner(innerSecurity),
                file.Path,
                excludeFields: PublisherBrokerFields.All)
            .Provide().KafkaRoutingConfig;

        routing.KafkaPublishingConfig.Server.Should().Be("inner:9092");
        routing.KafkaPublishingConfig.Properties["client.id"].Should().Be("inner-client");
        routing.KafkaPublishingConfig.Properties["compression.type"].Should().Be("snappy");
        routing.KafkaPublishingConfig.Properties["linger.ms"].Should().Be("33");
        routing.Security.Should().BeSameAs(innerSecurity);
    }

    [Fact]
    public void Cache_Result_Between_Calls()
    {
        const string BrokerJson = """{ "linger.ms": 0 }""";

        using var file = new TempJsonFile(BrokerJson);
        var provider = new JsonKafkaPublisherBrokerOverrideProvider(CreateInner(), file.Path);

        provider.Provide().Should().BeSameAs(provider.Provide());
    }

    [Fact]
    public void Throw_When_Broker_Json_Deserialises_To_Null()
    {
        using var file = new TempJsonFile("null");
        var provider = new JsonKafkaPublisherBrokerOverrideProvider(CreateInner(), file.Path);

        var act = () => provider.Provide();

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Apply_Only_Whitelisted_Keys_From_File()
    {
        // Whitelist allows linger.ms only; the catch-all deny blocks everything else.
        const string BrokerJson = """
        {
          "linger.ms": 99,
          "compression.type": "lz4",
          "acks": "all"
        }
        """;

        var whitelist = new List<KafkaConfigOverrideRule>
        {
            new() { Pattern = "linger.ms", Allow = true },
            new() { Pattern = "*", Allow = false },
        };

        using var file = new TempJsonFile(BrokerJson);
        var routing = new JsonKafkaPublisherBrokerOverrideProvider(CreateInner(), file.Path, whitelist: whitelist)
            .Provide().KafkaRoutingConfig;

        routing.KafkaPublishingConfig.Properties["linger.ms"].Should().Be("99");            // whitelisted → applied from file
        routing.KafkaPublishingConfig.Properties["compression.type"].Should().Be("snappy"); // denied → kept from inner
        routing.KafkaPublishingConfig.Properties["acks"].Should().Be("1");                  // denied → kept from inner
    }

    [Fact]
    public void Reject_All_File_Keys_When_Whitelist_Denies_All()
    {
        const string BrokerJson = """
        {
          "linger.ms": 99,
          "compression.type": "lz4"
        }
        """;

        var whitelist = new List<KafkaConfigOverrideRule>
        {
            new() { Pattern = "*", Allow = false },
        };

        using var file = new TempJsonFile(BrokerJson);
        var routing = new JsonKafkaPublisherBrokerOverrideProvider(CreateInner(), file.Path, whitelist: whitelist)
            .Provide().KafkaRoutingConfig;

        routing.KafkaPublishingConfig.Properties["linger.ms"].Should().Be("33");            // denied → kept from inner
        routing.KafkaPublishingConfig.Properties["compression.type"].Should().Be("snappy"); // denied → kept from inner
    }
}
