// <copyright file="KafkaListeningConfig.cs" company="Motion Applied Ltd.">
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
/// Kafka-specific consumer broker configuration. Routing-design settings
/// (<see cref="Server"/>, <see cref="GroupId"/>, <see cref="AutoOffsetResetMode"/>,
/// <see cref="Offset"/>) are typed fields; broker tuning is expressed as raw
/// librdkafka key/value pairs in <see cref="Properties"/>.
/// </summary>
public class KafkaListeningConfig
{
    public KafkaListeningConfig()
        : this("localhost:9092", Guid.NewGuid().ToString(), AutoOffsetResetMode.Latest, null)
    {
    }

    public KafkaListeningConfig(
        string server,
        string groupId,
        AutoOffsetResetMode autoOffsetResetMode,
        long? offset,
        IReadOnlyDictionary<string, string>? properties = null)
    {
        this.Server = server;
        this.GroupId = groupId;
        this.AutoOffsetResetMode = autoOffsetResetMode;
        this.Offset = offset;
        this.Properties = properties ?? new Dictionary<string, string>();
    }

    /// <summary>
    /// Raw librdkafka properties applied to the Confluent consumer config via <c>Set</c>.
    /// Keys not present here keep the Confluent/librdkafka default.
    /// </summary>
    public IReadOnlyDictionary<string, string> Properties { get; private set; }

    /// <summary>Returns a copy of this config with the given raw librdkafka <paramref name="properties"/>.</summary>
    public KafkaListeningConfig WithProperties(IReadOnlyDictionary<string, string> properties)
    {
        var clone = (KafkaListeningConfig)this.MemberwiseClone();
        clone.Properties = properties;
        return clone;
    }

    /// <summary>Bootstrap servers (comma-separated host:port).</summary>
    public string Server { get; }

    /// <summary>Consumer group identifier.</summary>
    public string GroupId { get; }

    public AutoOffsetResetMode AutoOffsetResetMode { get; }

    /// <summary>Explicit offset to start from when <see cref="AutoOffsetResetMode"/> is <c>FromOffset</c>.</summary>
    public long? Offset { get; }
}
