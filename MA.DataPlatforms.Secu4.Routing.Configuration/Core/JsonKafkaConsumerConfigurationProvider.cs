// <copyright file="JsonKafkaConsumerConfigurationProvider.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
/// Reads consumer configuration from a JSON file and exposes it as a
/// <see cref="ConsumingConfiguration"/>. The JSON structure keeps broker-specific
/// settings (<c>kafkaBroker</c> section) separate from routing-design settings
/// (<c>design</c> section).
///
/// A single <c>kafkaBroker</c> block applies to all consumer routes in the file.
/// Sensitive values in the <c>kafkaBroker.security</c> section support
/// <c>${PLACEHOLDER}</c> syntax. The <c>secretResolver</c> decides
/// where secrets are fetched from — environment variables by default, or any
/// vault / keystore via <see cref="MicrosoftConfigurationSecretResolver"/>.
/// </summary>
public class JsonKafkaConsumerConfigurationProvider : IConsumingConfigurationProvider
{
    private readonly string filePath;
    private readonly ISecretResolver secretResolver;
    private readonly IKafkaSecurityConfigProvider? securityConfigProvider;
    private ConsumingConfiguration? cached;

    /// <param name="filePath">
    /// Absolute or relative path to the consumer JSON config file.
    /// Defaults to <c>kafka-consumer.json</c> in the working directory.
    /// </param>
    /// <param name="secretResolver">
    /// Resolves <c>${PLACEHOLDER}</c> values in the security section.
    /// Defaults to <see cref="EnvironmentVariableSecretResolver"/>.
    /// Pass a <see cref="MicrosoftConfigurationSecretResolver"/> to pull
    /// secrets from Azure Key Vault, AWS Secrets Manager, HashiCorp Vault, etc.
    /// </param>
    /// <param name="securityConfigProvider">
    /// When provided, its result takes priority over the security section in the JSON file.
    /// Pass <see langword="null"/> to use the file-based security settings (default).
    /// </param>
    public JsonKafkaConsumerConfigurationProvider(
        string filePath = "kafka-consumer.json",
        ISecretResolver? secretResolver = null,
        IKafkaSecurityConfigProvider? securityConfigProvider = null)
    {
        this.filePath = filePath;
        this.secretResolver = secretResolver ?? new EnvironmentVariableSecretResolver();
        this.securityConfigProvider = securityConfigProvider;
    }

    public ConsumingConfiguration Provide()
    {
        if (this.cached is not null)
        {
            return this.cached;
        }

        var fileConfig = KafkaConfigJson.ReadFile<KafkaConsumerFileConfig>(this.filePath);

        this.cached = Map(fileConfig, this.secretResolver, this.securityConfigProvider?.Provide());
        return this.cached;
    }

    private static ConsumingConfiguration Map(KafkaConsumerFileConfig fc, ISecretResolver secrets, KafkaSecurityConfig? securityOverride)
    {
        var broker = fc.KafkaBroker;
        var design = fc.Design;
        var security = securityOverride ?? KafkaSecurityConfigMapper.Map(broker.Security, secrets);

        var consumingConfigs = design.Routes.Select(route =>
        {
            var offsetMode = route.AutoOffsetReset.ToLowerInvariant() switch
            {
                "earliest" => AutoOffsetResetMode.Earliest,
                "fromoffset" or "from_offset" => AutoOffsetResetMode.FromOffset,
                _ => AutoOffsetResetMode.Latest,
            };

            var listeningProperties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["enable.auto.commit"]        = broker.EnableAutoCommit.ToString().ToLowerInvariant(),
                ["auto.commit.interval.ms"]   = broker.AutoCommitIntervalMs.ToString(),
                ["session.timeout.ms"]        = broker.SessionTimeoutMs.ToString(),
                ["max.poll.interval.ms"]      = broker.MaxPollIntervalMs.ToString(),
                ["heartbeat.interval.ms"]     = broker.HeartbeatIntervalMs.ToString(),
                ["fetch.max.bytes"]           = broker.FetchMaxBytes.ToString(),
                ["max.partition.fetch.bytes"] = broker.MaxPartitionFetchBytes.ToString(),
            };
            if (!string.IsNullOrEmpty(broker.ClientId))
                listeningProperties["client.id"] = broker.ClientId;

            var listeningConfig = new KafkaListeningConfig(
                broker.BootstrapServers, broker.GroupId, offsetMode, route.FromOffset, listeningProperties);

            var kafkaRoute = new KafkaRoute(route.Name, route.Topic, route.Partition);

            var topicMetaData = design.TopicsMetaData
                .FirstOrDefault(m => m.Topic.Equals(route.Topic, StringComparison.OrdinalIgnoreCase));

            var routeMetaData = topicMetaData is not null
                ? new KafkaTopicMetaData(topicMetaData.Topic, topicMetaData.NumberOfPartitions, topicMetaData.ReplicationFactor)
                : new KafkaTopicMetaData(route.Topic);

            return new KafkaConsumingConfig(listeningConfig, kafkaRoute, routeMetaData, security);
        }).ToList();

        return new ConsumingConfiguration(consumingConfigs);
    }
}
