// <copyright file="Program.cs" company="McLaren Applied Ltd.">
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

using System.Diagnostics.CodeAnalysis;

namespace MA.DataPlatform.Secu4.Routing.Profiling;

[ExcludeFromCodeCoverage]
internal static class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("Testing Publishing Performance");
        var tester = new RouterTester(1_000_000, 130);
        tester.Setup();
        Console.WriteLine("Press Enter To Start Testing Publishing...");
        Console.ReadLine();
        Console.WriteLine($"Started:{DateTime.Now.TimeOfDay}");
        _ = Task.Run(tester.Start);
        WaitHandler.WaitEvent.WaitOne();
        Console.WriteLine($"finished:{DateTime.Now.TimeOfDay}");
        Console.ReadLine();
    }
}
