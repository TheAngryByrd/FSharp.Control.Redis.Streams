namespace FSharp.Control.Redis.Streams

module Akka =
    open System
    open Akka.Streams.Dsl
    open StackExchange.Redis
    open System.Threading.Tasks
    open FSharp.Control.Redis.Streams.Core
    open FSharp.Control.Tasks.V2.ContextInsensitive


    let inline taskUnfold (fn: 's -> Task<('s * 'e) option>) (state: 's) : Source<'e, unit> =
        Source.UnfoldAsync(state, Func<_, _>(fun x ->
            task {
                let! r = fn x
                match r with
                | Some tuple -> return tuple
                | None -> return Unchecked.defaultof<'s * 'e> })).MapMaterializedValue(Func<_,_>(ignore))

    let inline collect (fn: 't -> #seq<'u>) (source) : Source<'u, 'mat> =
        SourceOperations.SelectMany(source, Func<_, _>(fun x -> upcast fn x))

    let pollStreamForever (redisdb : IDatabase) (streamName : RedisKey) (startingPosition : RedisValue) (pollOptions : PollOptions) =
        let calculateNextPollDelay (nextPollDelay : TimeSpan) =
            let increment = (float pollOptions.MaxPollDelay.Ticks / pollOptions.MaxPollDelayBuckets)
            let nextPollDelay = nextPollDelay + TimeSpan.FromTicks(int64 increment)
            TimeSpan.Min nextPollDelay pollOptions.MaxPollDelay

        taskUnfold (fun (nextPosition, pollDelay) -> task {

            let! (response : StreamEntry []) = redisdb.StreamRangeAsync(streamName, minId = Nullable(nextPosition), count = (Option.toNullable pollOptions.CountToPullATime))
            match response with
            | EmptySeq ->
                let nextPollDelay = calculateNextPollDelay pollDelay
                do! Task.Delay pollDelay
                return Some ((nextPosition, nextPollDelay ) , Array.empty )
            | entries ->
                let lastEntry = Seq.last entries
                let nextPosition = EntryId.CalculateNextPosition lastEntry.Id
                let nextPollDelay = TimeSpan.Zero
                return Some ((nextPosition, nextPollDelay), entries )

        }) (startingPosition, TimeSpan.Zero)
        |> collect id

