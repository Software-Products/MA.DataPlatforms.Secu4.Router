// <copyright file="KafkaSecurityConfig.cs" company="Motion Applied Ltd.">
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

namespace MA.DataPlatforms.Secu4.Routing.Contracts;

/// <summary>
/// Broker-agnostic security configuration. Sensitive values (passwords, keys)
/// should be supplied at runtime via environment variables rather than stored
/// directly in config files.
/// </summary>
public class KafkaSecurityConfig
{
    public KafkaSecurityConfig(
        KafkaSecurityProtocol protocol = KafkaSecurityProtocol.Plaintext,
        KafkaSaslMechanism? saslMechanism = null,
        string? saslUsername = null,
        string? saslPassword = null,
        string? sslCaLocation = null,
        string? sslCertificateLocation = null,
        string? sslKeyLocation = null,
        string? sslKeyPassword = null)
    {
        this.Protocol = protocol;
        this.SaslMechanism = saslMechanism;
        this.SaslUsername = saslUsername;
        this.SaslPassword = saslPassword;
        this.SslCaLocation = sslCaLocation;
        this.SslCertificateLocation = sslCertificateLocation;
        this.SslKeyLocation = sslKeyLocation;
        this.SslKeyPassword = sslKeyPassword;
    }

    public KafkaSecurityProtocol Protocol { get; }

    public KafkaSaslMechanism? SaslMechanism { get; }

    public string? SaslUsername { get; }

    public string? SaslPassword { get; }

    public string? SslCaLocation { get; }

    public string? SslCertificateLocation { get; }

    public string? SslKeyLocation { get; }

    public string? SslKeyPassword { get; }
}

public enum KafkaSecurityProtocol
{
    Plaintext = 0,
    Ssl = 1,
    SaslPlaintext = 2,
    SaslSsl = 3,
}

public enum KafkaSaslMechanism
{
    Plain = 0,
    ScramSha256 = 1,
    ScramSha512 = 2,
    Gssapi = 3,
    OAuthBearer = 4,
}
