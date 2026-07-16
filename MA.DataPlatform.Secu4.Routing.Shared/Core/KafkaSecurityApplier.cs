// <copyright file="KafkaSecurityApplier.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Contracts;

namespace MA.DataPlatforms.Secu4.Routing.Shared.Core;

/// <summary>
/// Maps a <see cref="KafkaSecurityConfig"/> from the routing contracts onto a
/// Confluent <see cref="ClientConfig"/>. Used by both the producer and consumer
/// to avoid duplicating the protocol/mechanism mapping logic.
/// </summary>
public static class KafkaSecurityApplier
{
    public static void Apply(ClientConfig config, KafkaSecurityConfig? security)
    {
        if (security is null)
        {
            return;
        }

        config.SecurityProtocol = security.Protocol switch
        {
            KafkaSecurityProtocol.Ssl => SecurityProtocol.Ssl,
            KafkaSecurityProtocol.SaslPlaintext => SecurityProtocol.SaslPlaintext,
            KafkaSecurityProtocol.SaslSsl => SecurityProtocol.SaslSsl,
            _ => SecurityProtocol.Plaintext,
        };

        if (security.SaslMechanism.HasValue)
        {
            config.SaslMechanism = security.SaslMechanism.Value switch
            {
                KafkaSaslMechanism.ScramSha256 => SaslMechanism.ScramSha256,
                KafkaSaslMechanism.ScramSha512 => SaslMechanism.ScramSha512,
                KafkaSaslMechanism.Gssapi => SaslMechanism.Gssapi,
                KafkaSaslMechanism.OAuthBearer => SaslMechanism.OAuthBearer,
                _ => SaslMechanism.Plain,
            };
        }

        config.SaslUsername = security.SaslUsername;
        config.SaslPassword = security.SaslPassword;
        config.SslCaLocation = security.SslCaLocation;
        config.SslCertificateLocation = security.SslCertificateLocation;
        config.SslKeyLocation = security.SslKeyLocation;
        config.SslKeyPassword = security.SslKeyPassword;
    }
}
