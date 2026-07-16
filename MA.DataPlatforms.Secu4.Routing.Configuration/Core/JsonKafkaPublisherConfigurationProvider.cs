// <copyright file="JsonKafkaPublisherConfigurationProvider.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;
using MA.DataPlatforms.Secu4.Routing.Contracts;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
///     Reads publisher configuration from a JSON file and exposes it as a
///     <see cref="RoutingConfiguration" />. The JSON structure keeps broker-specific
///     settings (<c>kafkaBroker</c> section) separate from routing-design settings
///     (<c>design</c> section) so that changing the broker type only requires
///     replacing the broker section.
///     Sensitive values in the <c>kafkaBroker.security</c> section support
///     <c>${PLACEHOLDER}</c> syntax. The <c>secretResolver</c> decides
///     where secrets are fetched from — environment variables by default, or any
///     vault / keystore via <see cref="MicrosoftConfigurationSecretResolver" />.
/// </summary>
public class JsonKafkaPublisherConfigurationProvider : IRoutingConfigurationProvider
{
    private readonly string filePath;
    private readonly ISecretResolver secretResolver;
    private readonly IKafkaSecurityConfigProvider? securityConfigProvider;
    private RoutingConfiguration? cached;

    /// <param name="filePath">
    ///     Absolute or relative path to the publisher JSON config file.
    ///     Defaults to <c>kafka-publisher.json</c> in the working directory.
    /// </param>
    /// <param name="secretResolver">
    ///     Resolves <c>${PLACEHOLDER}</c> values in the security section.
    ///     Defaults to <see cref="EnvironmentVariableSecretResolver" />.
    ///     Pass a <see cref="MicrosoftConfigurationSecretResolver" /> to pull
    ///     secrets from Azure Key Vault, AWS Secrets Manager, HashiCorp Vault, etc.
    /// </param>
    /// <param name="securityConfigProvider">
    ///     When provided, its result takes priority over the security section in the JSON file.
    ///     Pass <see langword="null" /> to use the file-based security settings (default).
    /// </param>
    public JsonKafkaPublisherConfigurationProvider(
        string filePath = "kafka-publisher.json",
        ISecretResolver? secretResolver = null,
        IKafkaSecurityConfigProvider? securityConfigProvider = null)
    {
        this.filePath = filePath;
        this.secretResolver = secretResolver ?? new EnvironmentVariableSecretResolver();
        this.securityConfigProvider = securityConfigProvider;
    }

    public RoutingConfiguration Provide()
    {
        if (this.cached is not null)
        {
            return this.cached;
        }

        var fileConfig = KafkaConfigJson.ReadFile<KafkaPublisherFileConfig>(this.filePath);

        this.cached = Map(fileConfig, this.secretResolver, this.securityConfigProvider?.Provide());
        return this.cached;
    }

    private static RoutingConfiguration Map(KafkaPublisherFileConfig fc, ISecretResolver secrets, KafkaSecurityConfig? securityOverride)
    {
        var broker = fc.KafkaBroker;
        var design = fc.Design;

        var publishingProperties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["compression.type"] = broker.CompressionType,
            ["compression.level"] = broker.CompressionLevel.ToString(),
            ["linger.ms"] = broker.LingerMs.ToString(),
            ["queue.buffering.max.messages"] = broker.QueueBufferingMaxMessages.ToString(),
            ["queue.buffering.max.kbytes"] = broker.QueueBufferingMaxKbytes.ToString(),
            ["batch.size"] = broker.BatchSize.ToString(),
            ["acks"] = broker.Acks,
            ["max.in.flight.requests.per.connection"] = broker.MaxInFlight.ToString(),
            ["enable.idempotence"] = broker.EnableIdempotence.ToString().ToLowerInvariant(),
            ["request.timeout.ms"] = broker.RequestTimeoutMs.ToString(),
            ["message.max.bytes"] = broker.MessageMaxBytes.ToString(),
            ["message.timeout.ms"] = broker.MessageTimeoutMs.ToString(),
        };
        if (!string.IsNullOrEmpty(broker.ClientId))
        {
            publishingProperties["client.id"] = broker.ClientId;
        }

        var publishingConfig = new KafkaPublishingConfig(broker.BootstrapServers, publishingProperties);

        var routes = design.Routes
            .Select(r => new KafkaRoute(r.Name, r.Topic, r.Partition))
            .ToList();

        var metadata = design.TopicsMetaData
            .Select(m => new KafkaTopicMetaData(m.Topic, m.NumberOfPartitions, m.ReplicationFactor))
            .ToList();

        var security = securityOverride ?? KafkaSecurityConfigMapper.Map(broker.Security, secrets);

        var routingConfig = new KafkaRoutingConfig(
            kafkaPublishingConfig: publishingConfig,
            kafkaRoutes: routes,
            routesMetaData: metadata,
            deadLetterTopic: design.DeadLetterTopic,
            security: security);

        return new RoutingConfiguration(routingConfig);
    }
}
