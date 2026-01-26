// <copyright file="IRouteReadingWritingComponentRepository.cs" company="Motion Applied Ltd.">
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

namespace MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

public interface IRouteReadingWritingComponentRepository<T>
    where T : IRouteReadingWritingComponent
{
    void Add(T component);

    T? Get(string id);

    IReadOnlyList<T> GetAll();

    void Remove(string id);
}
