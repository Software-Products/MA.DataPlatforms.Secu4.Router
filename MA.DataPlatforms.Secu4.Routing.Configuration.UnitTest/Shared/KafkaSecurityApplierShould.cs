// <copyright file="KafkaSecurityApplierShould.cs" company="Motion Applied Ltd.">
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

using Confluent.Kafka;

using FluentAssertions;

using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

using Xunit;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Shared;

public class KafkaSecurityApplierShould
{
    [Fact]
    public void Leave_Config_Untouched_When_Security_Is_Null()
    {
        var config = new ClientConfig();

        KafkaSecurityApplier.Apply(config, null);

        config.SecurityProtocol.Should().BeNull();
        config.SaslMechanism.Should().BeNull();
        config.SaslUsername.Should().BeNull();
    }

    [Theory]
    [InlineData(KafkaSecurityProtocol.Plaintext, SecurityProtocol.Plaintext)]
    [InlineData(KafkaSecurityProtocol.Ssl, SecurityProtocol.Ssl)]
    [InlineData(KafkaSecurityProtocol.SaslPlaintext, SecurityProtocol.SaslPlaintext)]
    [InlineData(KafkaSecurityProtocol.SaslSsl, SecurityProtocol.SaslSsl)]
    public void Map_Security_Protocol(KafkaSecurityProtocol input, SecurityProtocol expected)
    {
        var config = new ClientConfig();

        KafkaSecurityApplier.Apply(config, new KafkaSecurityConfig(protocol: input));

        config.SecurityProtocol.Should().Be(expected);
    }

    [Theory]
    [InlineData(KafkaSaslMechanism.Plain, SaslMechanism.Plain)]
    [InlineData(KafkaSaslMechanism.ScramSha256, SaslMechanism.ScramSha256)]
    [InlineData(KafkaSaslMechanism.ScramSha512, SaslMechanism.ScramSha512)]
    [InlineData(KafkaSaslMechanism.Gssapi, SaslMechanism.Gssapi)]
    [InlineData(KafkaSaslMechanism.OAuthBearer, SaslMechanism.OAuthBearer)]
    public void Map_Sasl_Mechanism(KafkaSaslMechanism input, SaslMechanism expected)
    {
        var config = new ClientConfig();

        KafkaSecurityApplier.Apply(config, new KafkaSecurityConfig(
            protocol: KafkaSecurityProtocol.SaslSsl,
            saslMechanism: input));

        config.SaslMechanism.Should().Be(expected);
    }

    [Fact]
    public void Not_Set_Sasl_Mechanism_When_Not_Provided()
    {
        var config = new ClientConfig();

        KafkaSecurityApplier.Apply(config, new KafkaSecurityConfig(protocol: KafkaSecurityProtocol.Ssl));

        config.SaslMechanism.Should().BeNull();
    }

    [Fact]
    public void Copy_All_Credential_And_Ssl_Values()
    {
        var config = new ClientConfig();
        var security = new KafkaSecurityConfig(
            protocol: KafkaSecurityProtocol.SaslSsl,
            saslMechanism: KafkaSaslMechanism.ScramSha512,
            saslUsername: "user",
            saslPassword: "pass",
            sslCaLocation: "/ca.pem",
            sslCertificateLocation: "/cert.pem",
            sslKeyLocation: "/key.pem",
            sslKeyPassword: "key-pass");

        KafkaSecurityApplier.Apply(config, security);

        config.SaslUsername.Should().Be("user");
        config.SaslPassword.Should().Be("pass");
        config.SslCaLocation.Should().Be("/ca.pem");
        config.SslCertificateLocation.Should().Be("/cert.pem");
        config.SslKeyLocation.Should().Be("/key.pem");
        config.SslKeyPassword.Should().Be("key-pass");
    }
}
