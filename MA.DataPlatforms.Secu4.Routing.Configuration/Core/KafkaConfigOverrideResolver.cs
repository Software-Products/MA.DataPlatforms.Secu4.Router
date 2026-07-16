// <copyright file="KafkaConfigOverrideResolver.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
/// Resolves the effective set of raw librdkafka properties to apply to a Kafka client, by layering
/// a broker config file on top of a baseline. The result is a flat key/value map ready to be
/// <c>Set</c> directly onto a Confluent <c>ProducerConfig</c>/<c>ConsumerConfig</c> (keys not present
/// in the result keep the Confluent/librdkafka default).
///
/// <para>
/// Precedence per file key (highest first):
/// <list type="number">
///   <item><b>Excluded</b> — keys in <cref name="excludedKeys"/> are never taken from the file
///   (e.g. <c>bootstrap.servers</c>, <c>group.id</c>); the baseline value is kept.</item>
///   <item><b>Whitelist</b> — when a whitelist is supplied, the key must be allowed (see
///   <see cref="KafkaConfigOverrideWhitelistMatcher"/>, first-match-wins). No whitelist = all keys
///   allowed.</item>
///   <item><b>Present in file</b> — only keys physically present in the file override the baseline.</item>
/// </list>
/// </para>
/// </summary>
public static class KafkaConfigOverrideResolver
{
    public static Dictionary<string, string> Resolve(
        IReadOnlyDictionary<string, string> baseline,
        IReadOnlyDictionary<string, string> fileProperties,
        ISet<string> excludedKeys,
        IReadOnlyList<KafkaConfigOverrideRule>? whitelist)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in baseline)
        {
            result[key] = value;
        }

        foreach (var (key, value) in fileProperties)
        {
            if (excludedKeys.Contains(key))
            {
                continue;
            }

            if (!KafkaConfigOverrideWhitelistMatcher.IsAllowed(whitelist, key))
            {
                continue;
            }

            result[key] = value;
        }

        return result;
    }
}
