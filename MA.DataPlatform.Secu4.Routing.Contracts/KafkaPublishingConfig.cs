// <copyright file="KafkaPublishingConfig.cs" company="Motion Applied Ltd.">
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
/// Kafka-specific producer broker configuration. Broker tuning settings are
/// expressed as raw librdkafka key/value pairs in <see cref="Properties"/> and
/// applied to the Confluent <c>ProducerConfig</c> via <c>Set</c>.
/// </summary>
public class KafkaPublishingConfig
{
    public KafkaPublishingConfig()
        : this("localhost:9092")
    {
    }

    public KafkaPublishingConfig(string server, IReadOnlyDictionary<string, string>? properties = null)
    {
        this.Server = server;
        this.Properties = properties ?? new Dictionary<string, string>();
    }

    /// <summary>
    /// Raw librdkafka properties (e.g. <c>linger.ms</c>, <c>compression.type</c>) applied to the
    /// Confluent producer config via <c>Set</c>. Keys not present here keep the
    /// Confluent/librdkafka default.
    /// </summary>
    public IReadOnlyDictionary<string, string> Properties { get; private set; }

    /// <summary>Returns a copy of this config with the given raw librdkafka <paramref name="properties"/>.</summary>
    public KafkaPublishingConfig WithProperties(IReadOnlyDictionary<string, string> properties)
    {
        var clone = (KafkaPublishingConfig)this.MemberwiseClone();
        clone.Properties = properties;
        return clone;
    }

    /// <summary>Bootstrap servers (comma-separated host:port).</summary>
    public string Server { get; }
}
