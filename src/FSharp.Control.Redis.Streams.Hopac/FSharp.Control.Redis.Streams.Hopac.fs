namespace FSharp.Control.Redis.Streams

module Hopac =
    open System
    open Hopac
    open StackExchange.Redis
    open FSharp.Control.Redis.Streams.Core

    let pollStreamForever (redisdb : IDatabase) (streamName : RedisKey) (startingPosition : RedisValue) (pollOptions : PollOptions) =
        Stream.unfoldJob(fun (nextPosition, pollDelay) -> job {
            let! (response : StreamEntry []) = redisdb.StreamRangeAsync(streamName, minId = Nullable(nextPosition), count = (Option.toNullable pollOptions.CountToPullATime))
            match response with
            | EmptyArray ->
                let nextPollDelay = pollOptions.CalculateNextPollDelay pollDelay
                do! timeOut pollDelay
                return Some (Array.empty, (nextPosition, nextPollDelay ))
            | entries ->
                let lastEntry = Seq.last entries
                let nextPosition = EntryId.CalculateNextPosition lastEntry.Id
                let nextPollDelay = TimeSpan.Zero
                return Some (entries, (nextPosition, nextPollDelay))
        }) (startingPosition, TimeSpan.Zero)
        |> Stream.appendMap (Stream.ofSeq)


    let readFromStream (redisdb : IDatabase) (streamRead : ReadStreamConfig) =
        let startingPositiong =
            match streamRead.MessageOrder with
            | Order.Ascending -> streamRead.MinId |> Option.defaultValue StreamConstants.ReadMinValue
            | Order.Descending -> streamRead.MaxId |> Option.defaultValue StreamConstants.ReadMaxValue
            | _ -> failwith "If there's more than two directions in a stream the universe is broken, consult a physicist."

        Stream.unfoldJob(fun nextPosition -> job {
            let! (response : StreamEntry []) =
                redisdb.StreamRangeAsync(
                    key = streamRead.StreamName,
                    minId = (if streamRead.MessageOrder = Order.Ascending then Nullable(nextPosition) else Option.toNullable(streamRead.MinId)),
                    maxId = (if streamRead.MessageOrder = Order.Descending then Nullable(nextPosition) else Option.toNullable(streamRead.MaxId)),
                    count = Option.toNullable streamRead.CountToPullATime,
                    messageOrder = streamRead.MessageOrder,
                    flags = streamRead.Flags
                )
            match response with
            | EmptyArray ->
                return None
            | entries ->
                let lastEntry = Seq.last entries
                let nextPosition =
                    match streamRead.MessageOrder with
                    | Order.Ascending -> EntryId.CalculateNextPosition lastEntry.Id
                    | Order.Descending -> EntryId.CalculateNextPositionDesc lastEntry.Id
                    | _ -> failwith "If there's more than two directions in a stream the universe is broken, consult a physicist."
                return Some (entries, nextPosition)
        }) startingPositiong
        |> Stream.appendMap (Stream.ofSeq)
