// <copyright file="KafkaConsumerFileConfig.cs" company="Motion Applied Ltd.">
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
/// Root object for <c>kafka-consumer.json</c>.
///
/// The file is split into two independent sections:
/// <list type="bullet">
///   <item><description>
///     <see cref="Design"/> — which routes to subscribe to and from what
///     offset. These settings are broker-agnostic.
///   </description></item>
///   <item><description>
///     <see cref="KafkaBroker"/> — Confluent Kafka consumer settings (connection,
///     group management, fetch sizes, security). These settings are Kafka-specific.
///   </description></item>
/// </list>
/// </summary>
public class KafkaConsumerFileConfig
{
    /// <summary>Design-relative routing settings (broker-agnostic).</summary>
    public ConsumerDesignFileConfig Design { get; set; } = new();

    /// <summary>Kafka-specific consumer broker settings.</summary>
    public KafkaConsumerBrokerFileConfig KafkaBroker { get; set; } = new();
}
