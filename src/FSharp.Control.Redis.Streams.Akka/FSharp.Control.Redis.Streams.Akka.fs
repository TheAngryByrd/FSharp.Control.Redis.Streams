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

