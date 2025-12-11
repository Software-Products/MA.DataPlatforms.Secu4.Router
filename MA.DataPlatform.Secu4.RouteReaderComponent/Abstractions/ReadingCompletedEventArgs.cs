// <copyright file="ReadingCompletedEventArgs.cs" company="Motion Applied Ltd.">
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

namespace MA.DataPlatforms.Secu4.RouteReaderComponent.Abstractions;

public class ReadingCompletedEventArgs : EventArgs
{
    public ReadingCompletedEventArgs(string route, DateTime finishedTime, int numberOfItemsRead)
    {
        this.Route = route;
        this.FinishedTime = finishedTime;
        this.NumberOfItemsRead = numberOfItemsRead;
    }

    public string Route { get; }

    public DateTime FinishedTime { get; }

    public int NumberOfItemsRead { get; }
}
