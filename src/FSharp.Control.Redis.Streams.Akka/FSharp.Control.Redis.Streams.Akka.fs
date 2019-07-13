namespace FSharp.Control.Redis.Streams

module Akka =
    open System
    open Akka.Streams.Dsl
    open StackExchange.Redis
    open System.Threading.Tasks
    open FSharp.Control.Redis.Streams.Core
    open FSharp.Control.Tasks.V2.ContextInsensitive

    module Streams =
        let internal taskUnfold (fn: 's -> Task<('s * 'e) option>) (state: 's) : Source<'e, unit> =
            Source.UnfoldAsync(state, Func<_, _>(fun x ->
                task {
                    let! r = fn x
                    match r with
                    | Some tuple -> return tuple
                    | None -> return Unchecked.defaultof<'s * 'e> })).MapMaterializedValue(Func<_,_>(ignore))

        let internal collect (fn: 't -> #seq<'u>) (source) : Source<'u, 'mat> =
            SourceOperations.SelectMany(source, Func<_, _>(fun x -> upcast fn x))

    let pollStreamForever (redisdb : IDatabase) (streamName : RedisKey) (startingPosition : RedisValue) (pollOptions : PollOptions) =

        Streams.taskUnfold (fun (nextPosition, pollDelay) -> task {
            let! (response : StreamEntry []) = redisdb.StreamRangeAsync(streamName, minId = Nullable(nextPosition), count = (Option.toNullable pollOptions.CountToPullATime))
            match response with
            | EmptyArray ->
                let nextPollDelay = pollOptions.CalculateNextPollDelay pollDelay
                do! Task.Delay pollDelay
                return Some ((nextPosition, nextPollDelay ) , Array.empty )
            | entries ->
                let lastEntry = Seq.last entries
                let nextPosition = EntryId.CalculateNextPositionIncr lastEntry.Id
                let nextPollDelay = TimeSpan.Zero
                return Some ((nextPosition, nextPollDelay), entries )

        }) (startingPosition, TimeSpan.Zero)
        |> Streams.collect id



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

        let startingPosition =
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

        Streams.taskUnfold(fun nextPosition -> task {
            let! (response : StreamEntry []) = readStream nextPosition
            match response with
            | EmptyArray ->
                return None
            | entries ->
                let lastEntry = Seq.last entries
                let nextPosition = calculateNextPosition lastEntry.Id
                return Some (nextPosition, entries)
        }) startingPosition
        |> Streams.collect id
