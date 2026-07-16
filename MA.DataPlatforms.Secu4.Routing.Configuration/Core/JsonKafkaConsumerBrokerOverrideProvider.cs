// <copyright file="JsonKafkaConsumerBrokerOverrideProvider.cs" company="Motion Applied Ltd.">
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

using System.Text.Json;

using MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
/// Wraps an existing <see cref="IConsumingConfigurationProvider"/> and layers raw librdkafka consumer
/// properties from a JSON file onto every subscription. The file is a flat map of librdkafka keys
/// (e.g. <c>{ "session.timeout.ms": 45000 }</c>); only keys present in the file override,
/// everything else keeps the existing value (and ultimately the Confluent default).
///
/// <para>
/// An optional <c>"security"</c> object in the file is parsed and applied as the routing security
/// config. Use <see cref="ConsumerBrokerFields.Security"/> in <c>excludeFields</c> to keep the
/// inner provider's security instead.
/// </para>
///
/// <para>
/// Per-route design (route name, topic, partition, offset) and the bootstrap servers / group id are
/// preserved from the wrapped provider — they are always excluded from the file override. Additional
/// fields can be excluded via <cref name="excludeFields"/>. See <see cref="KafkaConfigOverrideResolver"/>.
/// </para>
/// </summary>
public class JsonKafkaConsumerBrokerOverrideProvider : IConsumingConfigurationProvider
{
    private readonly IConsumingConfigurationProvider inner;
    private readonly string brokerFilePath;
    private readonly ISet<string> excludedKeys;
    private readonly bool excludeSecurity;
    private readonly IReadOnlyList<KafkaConfigOverrideRule>? whitelist;
    private readonly IKafkaSecurityConfigProvider? securityConfigProvider;
    private readonly ISecretResolver secretResolver;
    private ConsumingConfiguration? cached;

    public JsonKafkaConsumerBrokerOverrideProvider(
        IConsumingConfigurationProvider inner,
        string brokerFilePath = "kafka-consumer-broker.json",
        ConsumerBrokerFields excludeFields = ConsumerBrokerFields.None,
        IReadOnlyList<KafkaConfigOverrideRule>? whitelist = null,
        IKafkaSecurityConfigProvider? securityConfigProvider = null,
        ISecretResolver? secretResolver = null)
    {
        this.inner = inner;
        this.brokerFilePath = brokerFilePath;
        this.excludedKeys = ToExcludedKeys(excludeFields);
        this.excludeSecurity = (excludeFields & ConsumerBrokerFields.Security) != 0;
        this.whitelist = whitelist;
        this.securityConfigProvider = securityConfigProvider;
        this.secretResolver = secretResolver ?? new EnvironmentVariableSecretResolver();
    }

    public ConsumingConfiguration Provide()
    {
        if (this.cached is not null)
        {
            return this.cached;
        }

        var existing = this.inner.Provide();
        var fileProperties = KafkaConfigJson.ReadRawProperties(this.brokerFilePath);

        KafkaSecurityConfig? overrideSecurity;
        if (this.securityConfigProvider is not null)
        {
            overrideSecurity = this.securityConfigProvider.Provide();
        }
        else if (!this.excludeSecurity)
        {
            overrideSecurity = ReadFileSecurity(this.brokerFilePath, this.secretResolver);
        }
        else
        {
            overrideSecurity = null;
        }

        var overriddenConfigs = existing.KafkaConsumingConfigs
            .Select(c =>
            {
                var mergedProperties = KafkaConfigOverrideResolver.Resolve(
                    c.KafkaListeningConfig.Properties,
                    fileProperties,
                    this.excludedKeys,
                    this.whitelist);

                var security = overrideSecurity ?? c.Security;

                return new KafkaConsumingConfig(
                    c.KafkaListeningConfig.WithProperties(mergedProperties),
                    c.KafkaRoutes,
                    c.RoutesMetaData,
                    security);
            })
            .ToList();

        this.cached = new ConsumingConfiguration(overriddenConfigs);
        return this.cached;
    }

    private static KafkaSecurityConfig? ReadFileSecurity(string path, ISecretResolver secrets)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        if (doc.RootElement.ValueKind == JsonValueKind.Object &&
            doc.RootElement.TryGetProperty("security", out var secElement))
        {
            var secConfig = JsonSerializer.Deserialize<KafkaBrokerSecurityFileConfig>(
                secElement.GetRawText(), KafkaConfigJson.Options);
            return secConfig is not null ? KafkaSecurityConfigMapper.Map(secConfig, secrets) : null;
        }

        return null;
    }

    private static readonly IReadOnlyDictionary<ConsumerBrokerFields, string> FieldToKey =
        new Dictionary<ConsumerBrokerFields, string>
        {
            [ConsumerBrokerFields.ClientId]                       = "client.id",
            [ConsumerBrokerFields.EnableAutoCommit]               = "enable.auto.commit",
            [ConsumerBrokerFields.AutoCommitIntervalMs]           = "auto.commit.interval.ms",
            [ConsumerBrokerFields.HeartbeatIntervalMs]            = "heartbeat.interval.ms",
            [ConsumerBrokerFields.SessionTimeoutMs]               = "session.timeout.ms",
            [ConsumerBrokerFields.MaxPollIntervalMs]              = "max.poll.interval.ms",
            [ConsumerBrokerFields.FetchMaxBytes]                  = "fetch.max.bytes",
            [ConsumerBrokerFields.MaxPartitionFetchBytes]         = "max.partition.fetch.bytes",
            [ConsumerBrokerFields.FetchMinBytes]                  = "fetch.min.bytes",
            [ConsumerBrokerFields.FetchWaitMaxMs]                 = "fetch.wait.max.ms",
            [ConsumerBrokerFields.BrokerAddressTtl]               = "broker.address.ttl",
            [ConsumerBrokerFields.QueuedMinMessages]              = "queued.min.messages",
            [ConsumerBrokerFields.QueuedMaxMessagesKbytes]        = "queued.max.messages.kbytes",
            [ConsumerBrokerFields.FetchErrorBackoffMs]            = "fetch.error.backoff.ms",
            [ConsumerBrokerFields.ReconnectBackoffMs]             = "reconnect.backoff.ms",
            [ConsumerBrokerFields.ReconnectBackoffMaxMs]          = "reconnect.backoff.max.ms",
            [ConsumerBrokerFields.SocketTimeoutMs]                = "socket.timeout.ms",
            [ConsumerBrokerFields.SocketKeepaliveEnable]          = "socket.keepalive.enable",
            [ConsumerBrokerFields.TopicMetadataRefreshIntervalMs] = "topic.metadata.refresh.interval.ms",
            [ConsumerBrokerFields.MetadataMaxAgeMs]               = "metadata.max.age.ms",
            [ConsumerBrokerFields.StatisticsIntervalMs]           = "statistics.interval.ms",
        };

    private static HashSet<string> ToExcludedKeys(ConsumerBrokerFields excludeFields)
    {
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            // Routing-design fields are always excluded.
            "bootstrap.servers",
            "group.id",
            "auto.offset.reset",
        };

        foreach (var (field, key) in FieldToKey)
        {
            if ((excludeFields & field) != 0)
                keys.Add(key);
        }

        return keys;
    }
}
