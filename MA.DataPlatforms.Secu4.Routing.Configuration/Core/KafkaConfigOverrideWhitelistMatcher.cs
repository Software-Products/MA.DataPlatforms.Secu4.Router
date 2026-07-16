// <copyright file="KafkaConfigOverrideWhitelistMatcher.cs" company="Motion Applied Ltd.">
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

using System.Text.RegularExpressions;

using MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
///     Decides whether a broker-config property is allowed to override the running Kafka configuration,
///     based on an ordered whitelist of wildcard rules (see <see cref="KafkaConfigOverrideWhitelist" />).
///     <para>
///         Strategy is <b>first match wins</b> ("first pass, first pick"): the rules are evaluated in order
///         and the first whose pattern matches any of the candidate property's names decides allow/deny.
///         A candidate can be matched by more than one name (e.g. its Confluent.Kafka C# property name
///         <c>LingerMs</c> and its dotted librdkafka key <c>linger.ms</c>) so patterns may be written in
///         either style. Matching is case-insensitive and supports <c>*</c> (any sequence) and <c>?</c>
///         (single character).
///     </para>
///     <para>
///         When the rule list is <see langword="null" /> or empty the property is allowed — an absent or
///         empty whitelist places no restriction, so all values replace (the default behaviour). When rules
///         exist but none match, the property is denied (whitelist semantics).
///     </para>
/// </summary>
public static class KafkaConfigOverrideWhitelistMatcher
{
    public static bool IsAllowed(IReadOnlyList<KafkaConfigOverrideRule>? rules, params string[] candidateNames)
    {
        if (rules is null ||
            rules.Count == 0)
        {
            return true;
        }

        return (from rule in rules
            where !string.IsNullOrWhiteSpace(rule.Pattern)
            let regex = GlobToRegex(rule.Pattern)
            where candidateNames.Any(name => !string.IsNullOrEmpty(name) && regex.IsMatch(name))
            select rule.Allow).FirstOrDefault();
    }

    private static Regex GlobToRegex(string pattern)
    {
        var escaped = Regex.Escape(pattern)
            .Replace("\\*", ".*")
            .Replace("\\?", ".");
        return new Regex($"^{escaped}$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
    }
}
