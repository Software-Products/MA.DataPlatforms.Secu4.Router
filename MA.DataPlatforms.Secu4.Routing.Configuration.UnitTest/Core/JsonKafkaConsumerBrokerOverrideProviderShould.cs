// <copyright file="JsonKafkaConsumerBrokerOverrideProviderShould.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

using NSubstitute;

using Xunit;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Core;

public class JsonKafkaConsumerBrokerOverrideProviderShould
{
    private static IConsumingConfigurationProvider CreateInner(KafkaSecurityConfig? security = null)
    {
        var listening = new KafkaListeningConfig(
            server: "inner:9092",
            groupId: "inner-group",
            autoOffsetResetMode: AutoOffsetResetMode.FromOffset,
            offset: 777,
            properties: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["client.id"]          = "inner-client",
                ["session.timeout.ms"] = "30000",
            });

        var config = new KafkaConsumingConfig(
            listening,
            new KafkaRoute("inner-route", "inner-topic", 4),
            new KafkaTopicMetaData("inner-topic", 5, 2),
            security);

        var inner = Substitute.For<IConsumingConfigurationProvider>();
        inner.Provide().Returns(new ConsumingConfiguration([config]));
        return inner;
    }

    [Fact]
    public void Override_Broker_Fields_But_Keep_Design()
    {
        // bootstrap.servers and group.id are always excluded — server and group stay from the inner provider.
        const string BrokerJson = """
        {
          "client.id": "file-client",
          "session.timeout.ms": 11111
        }
        """;

        using var file = new TempJsonFile(BrokerJson);
        var provider = new JsonKafkaConsumerBrokerOverrideProvider(CreateInner(), file.Path);

        var config = provider.Provide().KafkaConsumingConfigs[0];

        // Broker settings come from the file.
        config.KafkaListeningConfig.Properties["client.id"].Should().Be("file-client");
        config.KafkaListeningConfig.Properties["session.timeout.ms"].Should().Be("11111");

        // Design settings stay from the inner provider.
        config.KafkaListeningConfig.Server.Should().Be("inner:9092");
        config.KafkaListeningConfig.GroupId.Should().Be("inner-group");
        config.KafkaListeningConfig.AutoOffsetResetMode.Should().Be(AutoOffsetResetMode.FromOffset);
        config.KafkaListeningConfig.Offset.Should().Be(777);
        config.KafkaRoutes.Name.Should().Be("inner-route");
        config.KafkaRoutes.Topic.Should().Be("inner-topic");
        config.RoutesMetaData.NumberOfPartitions.Should().Be(5);
    }

    [Fact]
    public void Keep_Excluded_Fields_From_Inner_Provider()
    {
        const string BrokerJson = """
        {
          "client.id": "file-client",
          "session.timeout.ms": 11111
        }
        """;

        using var file = new TempJsonFile(BrokerJson);
        var provider = new JsonKafkaConsumerBrokerOverrideProvider(
            CreateInner(),
            file.Path,
            excludeFields: ConsumerBrokerFields.ClientId | ConsumerBrokerFields.SessionTimeoutMs);

        var listening = provider.Provide().KafkaConsumingConfigs[0].KafkaListeningConfig;

        listening.Properties["client.id"].Should().Be("inner-client");      // excluded -> kept from inner
        listening.Properties["session.timeout.ms"].Should().Be("30000");    // excluded -> kept from inner
    }

    [Fact]
    public void Map_Security_From_File_By_Default()
    {
        const string BrokerJson = """
        {
          "session.timeout.ms": 45000,
          "security": { "protocol": "SaslSsl", "saslMechanism": "Plain" }
        }
        """;

        using var file = new TempJsonFile(BrokerJson);
        var provider = new JsonKafkaConsumerBrokerOverrideProvider(CreateInner(), file.Path);

        provider.Provide().KafkaConsumingConfigs[0].Security!.Protocol.Should().Be(KafkaSecurityProtocol.SaslSsl);
    }

    [Fact]
    public void Keep_Inner_Security_When_Security_Field_Excluded()
    {
        const string BrokerJson = """
        {
          "session.timeout.ms": 45000,
          "security": { "protocol": "SaslSsl" }
        }
        """;

        var innerSecurity = new KafkaSecurityConfig(KafkaSecurityProtocol.Ssl);

        using var file = new TempJsonFile(BrokerJson);
        var provider = new JsonKafkaConsumerBrokerOverrideProvider(
            CreateInner(innerSecurity),
            file.Path,
            excludeFields: ConsumerBrokerFields.Security);

        provider.Provide().KafkaConsumingConfigs[0].Security.Should().BeSameAs(innerSecurity);
    }

    [Fact]
    public void Prefer_Security_Override_Provider()
    {
        const string BrokerJson = """
        {
          "session.timeout.ms": 45000,
          "security": { "protocol": "SaslSsl" }
        }
        """;

        var overrideSecurity = new KafkaSecurityConfig(KafkaSecurityProtocol.SaslPlaintext);
        var securityProvider = Substitute.For<IKafkaSecurityConfigProvider>();
        securityProvider.Provide().Returns(overrideSecurity);

        using var file = new TempJsonFile(BrokerJson);
        var provider = new JsonKafkaConsumerBrokerOverrideProvider(
            CreateInner(),
            file.Path,
            securityConfigProvider: securityProvider);

        provider.Provide().KafkaConsumingConfigs[0].Security.Should().BeSameAs(overrideSecurity);
    }

    [Fact]
    public void Keep_All_Broker_Fields_From_Inner_When_Excluding_All()
    {
        const string BrokerJson = """
        {
          "client.id": "file-client",
          "session.timeout.ms": 11111,
          "security": { "protocol": "SaslSsl" }
        }
        """;

        var innerSecurity = new KafkaSecurityConfig(KafkaSecurityProtocol.Ssl);

        using var file = new TempJsonFile(BrokerJson);
        var provider = new JsonKafkaConsumerBrokerOverrideProvider(
            CreateInner(innerSecurity),
            file.Path,
            excludeFields: ConsumerBrokerFields.All);

        var config = provider.Provide().KafkaConsumingConfigs[0];

        config.KafkaListeningConfig.Server.Should().Be("inner:9092");
        config.KafkaListeningConfig.GroupId.Should().Be("inner-group");
        config.KafkaListeningConfig.Properties["client.id"].Should().Be("inner-client");
        config.KafkaListeningConfig.Properties["session.timeout.ms"].Should().Be("30000");
        config.Security.Should().BeSameAs(innerSecurity);
    }

    [Fact]
    public void Cache_Result_Between_Calls()
    {
        const string BrokerJson = """{ "session.timeout.ms": 45000 }""";

        using var file = new TempJsonFile(BrokerJson);
        var provider = new JsonKafkaConsumerBrokerOverrideProvider(CreateInner(), file.Path);

        provider.Provide().Should().BeSameAs(provider.Provide());
    }

    [Fact]
    public void Throw_When_Broker_Json_Deserialises_To_Null()
    {
        using var file = new TempJsonFile("null");
        var provider = new JsonKafkaConsumerBrokerOverrideProvider(CreateInner(), file.Path);

        var act = () => provider.Provide();

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Apply_Only_Whitelisted_Keys_From_File()
    {
        // Whitelist allows session.timeout.ms only; the catch-all deny blocks everything else.
        const string BrokerJson = """
        {
          "session.timeout.ms": 11111,
          "client.id": "file-client"
        }
        """;

        var whitelist = new List<KafkaConfigOverrideRule>
        {
            new() { Pattern = "session.timeout.ms", Allow = true },
            new() { Pattern = "*", Allow = false },
        };

        using var file = new TempJsonFile(BrokerJson);
        var provider = new JsonKafkaConsumerBrokerOverrideProvider(CreateInner(), file.Path, whitelist: whitelist);
        var listening = provider.Provide().KafkaConsumingConfigs[0].KafkaListeningConfig;

        listening.Properties["session.timeout.ms"].Should().Be("11111");   // whitelisted → applied from file
        listening.Properties["client.id"].Should().Be("inner-client");      // denied → kept from inner
    }

    [Fact]
    public void Reject_All_File_Keys_When_Whitelist_Denies_All()
    {
        const string BrokerJson = """
        {
          "session.timeout.ms": 11111,
          "client.id": "file-client"
        }
        """;

        var whitelist = new List<KafkaConfigOverrideRule>
        {
            new() { Pattern = "*", Allow = false },
        };

        using var file = new TempJsonFile(BrokerJson);
        var provider = new JsonKafkaConsumerBrokerOverrideProvider(CreateInner(), file.Path, whitelist: whitelist);
        var listening = provider.Provide().KafkaConsumingConfigs[0].KafkaListeningConfig;

        listening.Properties["session.timeout.ms"].Should().Be("30000");   // denied → kept from inner
        listening.Properties["client.id"].Should().Be("inner-client");      // denied → kept from inner
    }
}
