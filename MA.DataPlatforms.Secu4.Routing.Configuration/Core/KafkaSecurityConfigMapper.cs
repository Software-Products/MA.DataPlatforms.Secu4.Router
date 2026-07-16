// <copyright file="KafkaSecurityConfigMapper.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;
using MA.DataPlatforms.Secu4.Routing.Contracts;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
/// Maps the file-based <see cref="KafkaBrokerSecurityFileConfig"/> security
/// section to the broker-agnostic <see cref="KafkaSecurityConfig"/>, resolving
/// any <c>${PLACEHOLDER}</c> secret values through the supplied
/// <see cref="ISecretResolver"/>.
///
/// Centralising the mapping here removes the previously duplicated logic from
/// every JSON configuration provider.
/// </summary>
internal static class KafkaSecurityConfigMapper
{
    /// <summary>
    /// Returns <see langword="null"/> when the section describes a plain,
    /// credential-free connection; otherwise returns a fully populated
    /// <see cref="KafkaSecurityConfig"/> with secrets resolved.
    /// </summary>
    public static KafkaSecurityConfig? Map(KafkaBrokerSecurityFileConfig sec, ISecretResolver secrets)
    {
        if (sec.Protocol.Equals("Plaintext", StringComparison.OrdinalIgnoreCase) &&
            sec.SaslMechanism is null &&
            sec.SaslUsername is null &&
            sec.SaslPassword is null &&
            sec.SslCaLocation is null &&
            sec.SslCertificateLocation is null &&
            sec.SslKeyLocation is null &&
            sec.SslKeyPassword is null)
        {
            return null;
        }

        var protocol = sec.Protocol.ToLowerInvariant() switch
        {
            "ssl" => KafkaSecurityProtocol.Ssl,
            "saslplaintext" or "sasl_plaintext" => KafkaSecurityProtocol.SaslPlaintext,
            "saslssl" or "sasl_ssl" => KafkaSecurityProtocol.SaslSsl,
            "plaintext" => KafkaSecurityProtocol.Plaintext,
            null => KafkaSecurityProtocol.Plaintext,
            _ => throw new NotImplementedException($"Kafka security protocol '{sec.Protocol}' is not supported.")
        };

        KafkaSaslMechanism? saslMechanism = sec.SaslMechanism?.ToLowerInvariant() switch
        {
            "scramsha256" or "scram-sha-256" => KafkaSaslMechanism.ScramSha256,
            "scramsha512" or "scram-sha-512" => KafkaSaslMechanism.ScramSha512,
            "gssapi" => KafkaSaslMechanism.Gssapi,
            "oauthbearer" => KafkaSaslMechanism.OAuthBearer,
            "plain" => KafkaSaslMechanism.Plain,
            null => KafkaSaslMechanism.Plain,
            _ => throw new NotImplementedException($"Kafka SASL mechanism '{sec.SaslMechanism}' is not supported.")
        };

        return new KafkaSecurityConfig(
            protocol: protocol,
            saslMechanism: saslMechanism,
            saslUsername: secrets.Resolve(sec.SaslUsername),
            saslPassword: secrets.Resolve(sec.SaslPassword),
            sslCaLocation: sec.SslCaLocation,
            sslCertificateLocation: sec.SslCertificateLocation,
            sslKeyLocation: secrets.Resolve(sec.SslKeyLocation),
            sslKeyPassword: secrets.Resolve(sec.SslKeyPassword));
    }
}
