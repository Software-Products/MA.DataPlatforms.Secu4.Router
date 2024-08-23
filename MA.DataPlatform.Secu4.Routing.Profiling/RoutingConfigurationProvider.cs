// <copyright file="RoutingConfigurationProvider.cs" company="McLaren Applied Ltd.">
//
// Copyright 2024 McLaren Applied Ltd
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

using System.Diagnostics.CodeAnalysis;

using MA.DataPlatform.Secu4.RouterComponent.Abstractions;
using MA.DataPlatform.Secu4.Routing.Contracts;

namespace MA.DataPlatform.Secu4.Routing.Profiling;

[ExcludeFromCodeCoverage]
public class RoutingConfigurationProvider : IRoutingConfigurationProvider
{
    public RoutingConfiguration Provide()
    {
        return new RoutingConfiguration(
            new KafkaRoutingConfig(
                new KafkaPublishingConfig("localhost:9092", -1, 0, 0, 10_000_000, 100_000_000),
                new List<KafkaRoute>
                {
                    new("test", "test")
                },
                new List<KafkaTopicMetaData>
                {
                    new("test")
                },
                "dead-letter"));
    }
}
