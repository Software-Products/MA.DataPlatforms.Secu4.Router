// <copyright file="Utility.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

namespace MA.DataPlatforms.Secu4.Routing.Shared.Core;

public class KafkaRoutingManagementInfoCreator : IKafkaRoutingManagementInfoCreator
{
    private readonly IConsumingConfigurationProvider configurationProvider;
    private readonly KafkaTopicMetaDataComparer comparer;

    public KafkaRoutingManagementInfoCreator(IConsumingConfigurationProvider configurationProvider)
    {
        this.configurationProvider = configurationProvider;
        this.comparer = new KafkaTopicMetaDataComparer(StringComparer.OrdinalIgnoreCase);
    }

    public List<KafkaRoutingManagementInfo> CreateRouteManagementInfo()
    {
        var config = this.configurationProvider.Provide();
        var kafkaRoutingManagementInfos = config.KafkaConsumingConfigs.Select(i => new
            {
                i.KafkaListeningConfig.Server,
                i.KafkaRoutes,
                i.RoutesMetaData
            }).GroupBy(i => i.Server).Select(i => new KafkaRoutingManagementInfo(
                i.Key,
                i.Select(j => j.KafkaRoutes).ToList(),
                i.Select(j => j.RoutesMetaData).Distinct(this.comparer).ToList()))
            .ToList();
        return kafkaRoutingManagementInfos;
    }
}
