// <copyright file="KafkaRouteRepository.cs" company="Motion Applied Ltd.">
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

using System.Collections.Concurrent;

using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Contracts.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

namespace MA.DataPlatforms.Secu4.Routing.Shared.Core;

public class KafkaRouteRepository : IKafkaRouteRepository
{
    private readonly ConcurrentDictionary<string, KafkaRoute> routeDictionary;
    private readonly List<KafkaRoute> routes;

    public KafkaRouteRepository(IEnumerable<KafkaRoute> kafkaRoutes)
    {
        this.routes = new List<KafkaRoute>(kafkaRoutes);
        this.routeDictionary = new ConcurrentDictionary<string, KafkaRoute>(this.routes.ToDictionary(i => i.Name));
    }

    public IRoute Find(string name)
    {
        if (this.routeDictionary.TryGetValue(name, out var route))
        {
            return route;
        }

        return new EmptyRoute();
    }

    public IReadOnlyList<IRoute> GetRoutes()
    {
        return this.routes;
    }
}
