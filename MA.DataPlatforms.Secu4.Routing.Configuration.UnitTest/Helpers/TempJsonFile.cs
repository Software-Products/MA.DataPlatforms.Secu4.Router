// <copyright file="TempJsonFile.cs" company="Motion Applied Ltd.">
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

namespace MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Helpers;

/// <summary>
/// Writes JSON content to a temporary file and deletes it on disposal so each
/// test runs against an isolated config file.
/// </summary>
internal sealed class TempJsonFile : IDisposable
{
    public TempJsonFile(string content)
    {
        this.Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"kafka-config-{Guid.NewGuid():N}.json");
        File.WriteAllText(this.Path, content);
    }

    public string Path { get; }

    public void Dispose()
    {
        if (File.Exists(this.Path))
        {
            File.Delete(this.Path);
        }
    }
}
