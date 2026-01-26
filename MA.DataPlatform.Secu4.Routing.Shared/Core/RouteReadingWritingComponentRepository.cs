// <copyright file="RouteReadingWritingComponentRepository.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

namespace MA.DataPlatforms.Secu4.Routing.Shared.Core;

public class RouteReadingWritingComponentRepository<T> : IRouteReadingWritingComponentRepository<T>
    where T : IRouteReadingWritingComponent
{
    private readonly ILogger logger;
    private readonly ConcurrentDictionary<string, T> components = new();

    public RouteReadingWritingComponentRepository(ILogger logger)
    {
        this.logger = logger;
    }

    public void Add(T component)
    {
        var found = this.Get(component.Id);
        if (found is not null)
        {
            this.logger.Error($"Cannot add Component with ID {component.Id} because it already exists in the repository.");
            return;
        }

        this.components.TryAdd(component.Id, component);
    }

    public T? Get(string id)
    {
        return this.components.GetValueOrDefault(id);
    }

    public IReadOnlyList<T> GetAll()
    {
        return this.components.Values.ToList();
    }

    public void Remove(string id)
    {
        this.components.TryRemove(id, out _);
    }
}
