// <copyright file="KafkaBrokerSecurityFileConfig.cs" company="Motion Applied Ltd.">
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

namespace MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;

/// <summary>
/// Security section of a Kafka broker config file.
/// Sensitive string values can reference environment variables using the
/// <c>${ENV_VAR_NAME}</c> placeholder syntax. The runtime resolves them
/// before passing values to the Confluent library so secrets never have to
/// be stored in the JSON files.
///
/// Example:
/// <code>
/// "security": {
///   "protocol": "SaslSsl",
///   "saslMechanism": "ScramSha512",
///   "saslUsername": "${KAFKA_USERNAME}",
///   "saslPassword": "${KAFKA_PASSWORD}"
/// }
/// </code>
/// </summary>
public class KafkaBrokerSecurityFileConfig
{
    /// <summary>
    /// Security protocol. One of: <c>Plaintext</c>, <c>Ssl</c>,
    /// <c>SaslPlaintext</c>, <c>SaslSsl</c>. Default: <c>Plaintext</c>.
    /// </summary>
    public string Protocol { get; set; } = "Plaintext";

    /// <summary>
    /// SASL mechanism. One of: <c>Plain</c>, <c>ScramSha256</c>,
    /// <c>ScramSha512</c>, <c>Gssapi</c>, <c>OAuthBearer</c>.
    /// Required when Protocol includes SASL.
    /// </summary>
    public string? SaslMechanism { get; set; }

    /// <summary>SASL username. Supports <c>${ENV_VAR}</c> placeholder.</summary>
    public string? SaslUsername { get; set; }

    /// <summary>SASL password. Supports <c>${ENV_VAR}</c> placeholder.</summary>
    public string? SaslPassword { get; set; }

    /// <summary>Path to CA certificate file for SSL verification.</summary>
    public string? SslCaLocation { get; set; }

    /// <summary>Path to client certificate file for mutual TLS.</summary>
    public string? SslCertificateLocation { get; set; }

    /// <summary>Path to client private key file. Supports <c>${ENV_VAR}</c> placeholder.</summary>
    public string? SslKeyLocation { get; set; }

    /// <summary>Passphrase for the private key. Supports <c>${ENV_VAR}</c> placeholder.</summary>
    public string? SslKeyPassword { get; set; }
}
