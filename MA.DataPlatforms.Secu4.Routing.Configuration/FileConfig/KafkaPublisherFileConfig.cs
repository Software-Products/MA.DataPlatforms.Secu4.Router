// <copyright file="KafkaPublisherFileConfig.cs" company="Motion Applied Ltd.">
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
/// Root object for <c>kafka-publisher.json</c>.
///
/// The file is split into two independent sections:
/// <list type="bullet">
///   <item><description>
///     <see cref="Design"/> — routing topology (routes, dead-letter topic,
///     topic metadata). These settings are broker-agnostic and remain valid
///     regardless of which message broker is used.
///   </description></item>
///   <item><description>
///     <see cref="KafkaBroker"/> — Confluent Kafka producer settings (connection,
///     compression, batching, acks, security). These settings are Kafka-specific
///     and would be replaced if the broker technology changes.
///   </description></item>
/// </list>
/// </summary>
public class KafkaPublisherFileConfig
{
    /// <summary>Design-relative routing settings (broker-agnostic).</summary>
    public PublisherDesignFileConfig Design { get; set; } = new();

    /// <summary>Kafka-specific producer broker settings.</summary>
    public KafkaProducerBrokerFileConfig KafkaBroker { get; set; } = new();
}
