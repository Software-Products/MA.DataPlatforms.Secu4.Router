// <copyright file="KafkaRoutingConfig.cs" company="McLaren Applied Ltd.">
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

namespace MA.DataPlatform.Secu4.Routing.Contracts;

public class KafkaRoutingConfig
{
    public KafkaRoutingConfig(
        KafkaPublishingConfig kafkaPublishingConfig,
        List<KafkaRoute> kafkaRoutes,
        List<KafkaTopicMetaData> routesMetaData,
        string deadLetterTopic)
    {
        this.KafkaPublishingConfig = kafkaPublishingConfig;
        this.KafkaRoutes = kafkaRoutes;
        this.RoutesMetaData = routesMetaData;
        this.DeadLetterTopic = deadLetterTopic;
    }

    public KafkaPublishingConfig KafkaPublishingConfig { get; }

    public List<KafkaRoute> KafkaRoutes { get; }

    public List<KafkaTopicMetaData> RoutesMetaData { get; }

    public string DeadLetterTopic { get; }
}
