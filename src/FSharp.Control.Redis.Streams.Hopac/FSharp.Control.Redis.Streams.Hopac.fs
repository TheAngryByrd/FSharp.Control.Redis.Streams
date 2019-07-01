namespace FSharp.Control.Redis.Streams

module Hopac =
    open System
    open Hopac
    open StackExchange.Redis
    open FSharp.Control.Redis.Streams.Core

    let pollStreamForever (redisdb : IDatabase) (streamName : RedisKey) (startingPosition : RedisValue) (pollOptions : PollOptions) =
        let calculateNextPollDelay (nextPollDelay) =
            let increment = (float pollOptions.MaxPollDelay.Ticks / pollOptions.MaxPollDelayBuckets)
            let nextPollDelay = nextPollDelay + TimeSpan.FromTicks(int64 increment)
            TimeSpan.Min nextPollDelay pollOptions.MaxPollDelay

        Stream.unfoldJob(fun (nextPosition, pollDelay) -> job {
            let! (response : StreamEntry []) = redisdb.StreamRangeAsync(streamName, minId = Nullable(nextPosition), count = (Option.toNullable pollOptions.CountToPullATime))
            match response with
            | EmptySeq ->
                let nextPollDelay = calculateNextPollDelay pollDelay
                do! timeOut pollDelay
                return Some (Array.empty, (nextPosition, nextPollDelay ))
            | entries ->
                let lastEntry = Seq.last entries
                let nextPosition = EntryId.CalculateNextPosition lastEntry.Id
                let nextPollDelay = TimeSpan.Zero
                return Some (entries, (nextPosition, nextPollDelay))
        }) (startingPosition, TimeSpan.FromMilliseconds(0.))
        |> Stream.appendMap (Stream.ofSeq)
