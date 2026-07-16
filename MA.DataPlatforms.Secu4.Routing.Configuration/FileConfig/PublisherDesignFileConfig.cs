// <copyright file="PublisherDesignFileConfig.cs" company="Motion Applied Ltd.">
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
/// Design-relative publisher settings: routing topology and dead-letter
/// behaviour. These values describe *what* to route and *where*, and are
/// completely independent of the underlying broker technology.
/// </summary>
public class PublisherDesignFileConfig
{
    /// <summary>
    /// Topic that receives messages whose route name is not in the routing
    /// table. Default: <c>dead-letter</c>.
    /// </summary>
    public string DeadLetterTopic { get; set; } = "dead-letter";

    /// <summary>
    /// Routing table: maps logical route names to Kafka topics (and
    /// optionally to a specific partition).
    /// </summary>
    public List<RouteFileConfig> Routes { get; set; } = [];

    /// <summary>
    /// Metadata used for automatic topic creation. Each entry must match a
    /// topic referenced in <see cref="Routes"/> or <see cref="DeadLetterTopic"/>.
    /// </summary>
    public List<TopicMetaDataFileConfig> TopicsMetaData { get; set; } = [];
}

/// <summary>Maps a logical route name to a Kafka topic.</summary>
public class RouteFileConfig
{
    /// <summary>Logical route name used in publish calls.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Kafka topic to publish messages to.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Optional specific partition. When null, the producer's partitioner
    /// decides (round-robin / key-hash). Default: <c>null</c>.
    /// </summary>
    public int? Partition { get; set; }
}

/// <summary>Kafka topic auto-creation parameters.</summary>
public class TopicMetaDataFileConfig
{
    /// <summary>Kafka topic name.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Number of partitions to create. When null, the broker default is
    /// used. Default: <c>null</c>.
    /// </summary>
    public int? NumberOfPartitions { get; set; }

    /// <summary>
    /// Replication factor. When null, the broker default is used.
    /// Default: <c>null</c>.
    /// </summary>
    public short? ReplicationFactor { get; set; }
}
