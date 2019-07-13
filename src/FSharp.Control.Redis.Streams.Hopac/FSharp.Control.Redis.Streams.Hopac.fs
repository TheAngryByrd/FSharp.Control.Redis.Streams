namespace FSharp.Control.Redis.Streams

module Hopac =
    open System
    open Hopac
    open StackExchange.Redis
    open FSharp.Control.Redis.Streams.Core

    let pollStreamForever (redisdb : IDatabase) (streamName : RedisKey) (startingPosition : RedisValue) (pollOptions : PollOptions) =
        Stream.unfoldJob(fun (nextPosition, pollDelay) -> job {
            let! (response : StreamEntry []) =
                redisdb.StreamRangeAsync(
                    streamName,
                    minId = Nullable(nextPosition),
                    count = (Option.toNullable pollOptions.CountToPullATime))

            match response with
            | EmptyArray ->
                let nextPollDelay = pollOptions.CalculateNextPollDelay pollDelay
                do! timeOut pollDelay
                return Some (Array.empty, (nextPosition, nextPollDelay ))
            | entries ->
                let lastEntry = Seq.last entries
                let nextPosition = EntryId.CalculateNextPositionIncr lastEntry.Id
                let nextPollDelay = TimeSpan.Zero
                return Some (entries, (nextPosition, nextPollDelay))
        }) (startingPosition, TimeSpan.Zero)
        |> Stream.appendMap (Stream.ofSeq)


    let readFromStream (redisdb : IDatabase) (streamRead : ReadStreamConfig) =
        let readForward (newMinId : RedisValue) =
            redisdb.StreamRangeAsync(
                key = streamRead.StreamName,
                minId = Nullable newMinId,
                maxId = Option.toNullable(streamRead.MaxId),
                count = Option.toNullable streamRead.CountToPullATime,
                messageOrder = streamRead.MessageOrder,
                flags = streamRead.Flags
            )

        let readBackward (newMaxId : RedisValue) =
            redisdb.StreamRangeAsync(
                key = streamRead.StreamName,
                minId = Option.toNullable(streamRead.MinId),
                maxId = Nullable newMaxId,
                count = Option.toNullable streamRead.CountToPullATime,
                messageOrder = streamRead.MessageOrder,
                flags = streamRead.Flags
            )

        let failureForMessageOrderCheck () =
            failwith "If there's more than two directions in a stream the universe is broken, consult a physicist."

        let startingPositiong =
            match streamRead.MessageOrder with
            | Order.Ascending -> streamRead.MinId |> Option.defaultValue StreamConstants.ReadMinValue
            | Order.Descending -> streamRead.MaxId |> Option.defaultValue StreamConstants.ReadMaxValue
            | _ -> failureForMessageOrderCheck ()

        let readStream =
            match streamRead.MessageOrder with
            | Order.Ascending -> readForward
            | Order.Descending -> readBackward
            | _ -> failureForMessageOrderCheck ()

        let calculateNextPosition =
            match streamRead.MessageOrder with
            | Order.Ascending -> EntryId.CalculateNextPositionIncr
            | Order.Descending -> EntryId.CalculateNextPositionDesc
            | _ -> failureForMessageOrderCheck ()

        Stream.unfoldJob(fun nextPosition -> job {
            let! (response : StreamEntry []) = readStream nextPosition
            match response with
            | EmptyArray ->
                return None
            | entries ->
                let lastEntry = Seq.last entries
                let nextPosition = calculateNextPosition lastEntry.Id
                return Some (entries, nextPosition)
        }) startingPositiong
        |> Stream.appendMap (Stream.ofSeq)
