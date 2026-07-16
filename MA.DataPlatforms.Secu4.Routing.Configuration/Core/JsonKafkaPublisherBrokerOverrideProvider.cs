// <copyright file="JsonKafkaPublisherBrokerOverrideProvider.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
/// Wraps an existing <see cref="IRoutingConfigurationProvider"/> and layers raw librdkafka producer
/// properties from a JSON file onto it. The file is a flat map of librdkafka keys
/// (e.g. <c>{ "linger.ms": 0, "compression.type": "zstd" }</c>); only keys present in the file
/// override, everything else keeps the existing value (and ultimately the Confluent default).
///
/// <para>
/// An optional <c>"security"</c> object in the file is parsed and applied as the routing security
/// config. Use <see cref="PublisherBrokerFields.Security"/> in <c>excludeFields</c> to keep the
/// inner provider's security instead.
/// </para>
///
/// <para>
/// The routing design (routes, dead-letter topic, topic metadata) and the bootstrap servers are
/// preserved from the wrapped provider. Which keys may override is governed by
/// <cref name="excludeFields"/>. See <see cref="KafkaConfigOverrideResolver"/>.
/// </para>
/// </summary>
public class JsonKafkaPublisherBrokerOverrideProvider : IRoutingConfigurationProvider
{
    private readonly IRoutingConfigurationProvider inner;
    private readonly string brokerFilePath;
    private readonly ISet<string> excludedKeys;
    private readonly bool excludeSecurity;
    private readonly IReadOnlyList<KafkaConfigOverrideRule>? whitelist;
    private readonly IKafkaSecurityConfigProvider? securityConfigProvider;
    private readonly ISecretResolver secretResolver;
    private RoutingConfiguration? cached;

    public JsonKafkaPublisherBrokerOverrideProvider(
        IRoutingConfigurationProvider inner,
        string brokerFilePath = "kafka-publisher-broker.json",
        PublisherBrokerFields excludeFields = PublisherBrokerFields.None,
        IReadOnlyList<KafkaConfigOverrideRule>? whitelist = null,
        IKafkaSecurityConfigProvider? securityConfigProvider = null,
        ISecretResolver? secretResolver = null)
    {
        this.inner = inner;
        this.brokerFilePath = brokerFilePath;
        this.excludedKeys = ToExcludedKeys(excludeFields);
        this.excludeSecurity = (excludeFields & PublisherBrokerFields.Security) != 0;
        this.whitelist = whitelist;
        this.securityConfigProvider = securityConfigProvider;
        this.secretResolver = secretResolver ?? new EnvironmentVariableSecretResolver();
    }

    public RoutingConfiguration Provide()
    {
        if (this.cached is not null)
        {
            return this.cached;
        }

        var existingRouting = this.inner.Provide().KafkaRoutingConfig;
        var existingPc = existingRouting.KafkaPublishingConfig;

        var fileProperties = KafkaConfigJson.ReadRawProperties(this.brokerFilePath);
        var mergedProperties = KafkaConfigOverrideResolver.Resolve(
            existingPc.Properties,
            fileProperties,
            this.excludedKeys,
            this.whitelist);

        KafkaSecurityConfig? security;
        if (this.securityConfigProvider is not null)
        {
            security = this.securityConfigProvider.Provide();
        }
        else if (!this.excludeSecurity)
        {
            security = ReadFileSecurity(this.brokerFilePath, this.secretResolver) ?? existingRouting.Security;
        }
        else
        {
            security = existingRouting.Security;
        }

        this.cached = new RoutingConfiguration(new KafkaRoutingConfig(
            kafkaPublishingConfig: existingPc.WithProperties(mergedProperties),
            kafkaRoutes: existingRouting.KafkaRoutes,
            routesMetaData: existingRouting.RoutesMetaData,
            deadLetterTopic: existingRouting.DeadLetterTopic,
            security: security));

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

    private static readonly IReadOnlyDictionary<PublisherBrokerFields, string> FieldToKey =
        new Dictionary<PublisherBrokerFields, string>
        {
            [PublisherBrokerFields.ClientId]                       = "client.id",
            [PublisherBrokerFields.CompressionType]                = "compression.type",
            [PublisherBrokerFields.CompressionLevel]               = "compression.level",
            [PublisherBrokerFields.LingerMs]                       = "linger.ms",
            [PublisherBrokerFields.BatchSize]                      = "batch.size",
            [PublisherBrokerFields.QueueBufferingMaxMessages]      = "queue.buffering.max.messages",
            [PublisherBrokerFields.QueueBufferingMaxKbytes]        = "queue.buffering.max.kbytes",
            [PublisherBrokerFields.MessageMaxBytes]                = "message.max.bytes",
            [PublisherBrokerFields.MessageTimeoutMs]               = "message.timeout.ms",
            [PublisherBrokerFields.RequestTimeoutMs]               = "request.timeout.ms",
            [PublisherBrokerFields.Acks]                           = "acks",
            [PublisherBrokerFields.MaxInFlight]                    = "max.in.flight.requests.per.connection",
            [PublisherBrokerFields.EnableIdempotence]              = "enable.idempotence",
            [PublisherBrokerFields.RetryBackoffMs]                 = "retry.backoff.ms",
            [PublisherBrokerFields.RetryBackoffMaxMs]              = "retry.backoff.max.ms",
            [PublisherBrokerFields.MessageSendMaxRetries]          = "message.send.max.retries",
            [PublisherBrokerFields.BrokerAddressTtl]               = "broker.address.ttl",
            [PublisherBrokerFields.ReconnectBackoffMs]             = "reconnect.backoff.ms",
            [PublisherBrokerFields.ReconnectBackoffMaxMs]          = "reconnect.backoff.max.ms",
            [PublisherBrokerFields.SocketTimeoutMs]                = "socket.timeout.ms",
            [PublisherBrokerFields.SocketKeepaliveEnable]          = "socket.keepalive.enable",
            [PublisherBrokerFields.TopicMetadataRefreshIntervalMs] = "topic.metadata.refresh.interval.ms",
            [PublisherBrokerFields.MetadataMaxAgeMs]               = "metadata.max.age.ms",
            [PublisherBrokerFields.StatisticsIntervalMs]           = "statistics.interval.ms",
        };

    private static HashSet<string> ToExcludedKeys(PublisherBrokerFields excludeFields)
    {
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            // bootstrap.servers is always excluded — it is a routing-design field.
            "bootstrap.servers",
        };

        foreach (var (field, key) in FieldToKey)
        {
            if ((excludeFields & field) != 0)
                keys.Add(key);
        }

        return keys;
    }
}
