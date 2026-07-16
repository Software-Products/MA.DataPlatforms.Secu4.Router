// <copyright file="KafkaConfigOverrideResolverShould.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;

using Xunit;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Core;

public class KafkaConfigOverrideResolverShould
{
    private static readonly Dictionary<string, string> Baseline = new(StringComparer.OrdinalIgnoreCase)
    {
        ["compression.type"] = "zstd",
        ["linger.ms"] = "0",
        ["acks"] = "0",
    };

    [Fact]
    public void Keep_Baseline_When_File_Is_Empty()
    {
        var result = KafkaConfigOverrideResolver.Resolve(Baseline, new Dictionary<string, string>(), Excluded(), null);

        result.Should().BeEquivalentTo(Baseline);
    }

    [Fact]
    public void Override_Only_The_Keys_Present_In_The_File()
    {
        var file = new Dictionary<string, string> { ["linger.ms"] = "100" };

        var result = KafkaConfigOverrideResolver.Resolve(Baseline, file, Excluded(), null);

        result["linger.ms"].Should().Be("100");      // overridden
        result["compression.type"].Should().Be("zstd"); // baseline kept
        result["acks"].Should().Be("0");                 // baseline kept
    }

    [Fact]
    public void Add_File_Keys_That_Are_Not_In_The_Baseline()
    {
        var file = new Dictionary<string, string> { ["batch.size"] = "1048576" };

        var result = KafkaConfigOverrideResolver.Resolve(Baseline, file, Excluded(), null);

        result["batch.size"].Should().Be("1048576");
    }

    [Fact]
    public void Never_Apply_Excluded_Keys_From_The_File()
    {
        var file = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "evil:9092",
            ["linger.ms"] = "100",
        };

        var result = KafkaConfigOverrideResolver.Resolve(Baseline, file, Excluded("bootstrap.servers"), null);

        result.ContainsKey("bootstrap.servers").Should().BeFalse(); // excluded — not taken from file
        result["linger.ms"].Should().Be("100");                     // non-excluded still applied
    }

    [Fact]
    public void Apply_Only_Whitelisted_Keys_When_A_Whitelist_Is_Supplied()
    {
        var file = new Dictionary<string, string>
        {
            ["linger.ms"] = "100",
            ["batch.size"] = "1048576",
        };
        var whitelist = new List<KafkaConfigOverrideRule>
        {
            new() { Pattern = "linger.ms", Allow = true },
            new() { Pattern = "*", Allow = false },
        };

        var result = KafkaConfigOverrideResolver.Resolve(Baseline, file, Excluded(), whitelist);

        result["linger.ms"].Should().Be("100");        // whitelisted
        result.ContainsKey("batch.size").Should().BeFalse(); // denied by catch-all
    }

    [Fact]
    public void Apply_All_File_Keys_When_No_Whitelist()
    {
        var file = new Dictionary<string, string>
        {
            ["linger.ms"] = "100",
            ["batch.size"] = "1048576",
        };

        var result = KafkaConfigOverrideResolver.Resolve(Baseline, file, Excluded(), null);

        result["linger.ms"].Should().Be("100");
        result["batch.size"].Should().Be("1048576");
    }

    private static HashSet<string> Excluded(params string[] keys) =>
        new HashSet<string>(keys, StringComparer.OrdinalIgnoreCase);
}
