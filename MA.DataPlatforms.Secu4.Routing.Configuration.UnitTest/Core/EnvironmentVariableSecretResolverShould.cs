// <copyright file="EnvironmentVariableSecretResolverShould.cs" company="Motion Applied Ltd.">
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

using FluentAssertions;

using MA.DataPlatforms.Secu4.Routing.Configuration.Core;

using Xunit;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Core;

public class EnvironmentVariableSecretResolverShould
{
    private readonly EnvironmentVariableSecretResolver resolver = new();

    [Fact]
    public void Return_Null_When_Value_Is_Null()
    {
        this.resolver.Resolve(null).Should().BeNull();
    }

    [Fact]
    public void Return_Value_Unchanged_When_Not_A_Placeholder()
    {
        this.resolver.Resolve("plain-value").Should().Be("plain-value");
    }

    [Fact]
    public void Return_Value_Unchanged_When_Placeholder_Is_Malformed()
    {
        // Missing closing brace - not a full-string placeholder.
        this.resolver.Resolve("${NOT_CLOSED").Should().Be("${NOT_CLOSED");
    }

    [Fact]
    public void Resolve_Placeholder_From_Environment_Variable()
    {
        var name = $"SECU4_TEST_{Guid.NewGuid():N}";
        Environment.SetEnvironmentVariable(name, "resolved-secret");
        try
        {
            this.resolver.Resolve($"${{{name}}}").Should().Be("resolved-secret");
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Fact]
    public void Return_Null_When_Environment_Variable_Is_Missing()
    {
        var name = $"SECU4_MISSING_{Guid.NewGuid():N}";
        this.resolver.Resolve($"${{{name}}}").Should().BeNull();
    }
}
