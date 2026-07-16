// <copyright file="ConsumerDesignFileConfig.cs" company="Motion Applied Ltd.">
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
/// Design-relative consumer settings: which routes to listen on and from
/// what offset. These are broker-agnostic routing decisions.
/// </summary>
public class ConsumerDesignFileConfig
{
    /// <summary>
    /// Routes to subscribe to. Each entry becomes an independent consumer
    /// with its own offset tracking.
    /// </summary>
    public List<ConsumerRouteFileConfig> Routes { get; set; } = [];

    /// <summary>
    /// Metadata used for automatic topic creation if the topic does not
    /// already exist. Each entry should match a topic referenced in
    /// <see cref="Routes"/>.
    /// </summary>
    public List<TopicMetaDataFileConfig> TopicsMetaData { get; set; } = [];
}

/// <summary>
/// Describes a single route a consumer should listen on, including offset
/// behaviour which is a design decision (not a broker-level setting).
/// </summary>
public class ConsumerRouteFileConfig
{
    /// <summary>Logical route name used to identify received messages.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Kafka topic to consume from.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Optional specific partition. When null, all partitions of the topic
    /// are consumed. Default: <c>null</c>.
    /// </summary>
    public int? Partition { get; set; }

    /// <summary>
    /// Where to start reading when no committed offset exists for this
    /// consumer group. One of: <c>Latest</c>, <c>Earliest</c>,
    /// <c>FromOffset</c>. Default: <c>Latest</c>.
    /// </summary>
    public string AutoOffsetReset { get; set; } = "Latest";

    /// <summary>
    /// Explicit offset to start from. Only used when
    /// <see cref="AutoOffsetReset"/> is <c>FromOffset</c>.
    /// Default: <c>null</c>.
    /// </summary>
    public long? FromOffset { get; set; }
}
