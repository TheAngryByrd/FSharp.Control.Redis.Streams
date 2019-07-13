namespace FSharp.Control.Redis.Streams

module Reactive =
    open System
    open System.Threading
    open System.Threading.Tasks
    open System.Reactive
    open System.Reactive.Linq
    open StackExchange.Redis
    open FSharp.Control.Redis.Streams.Core
    open FSharp.Control.Tasks.V2.ContextInsensitive

    module Observable =
        let taskUnfold (fn: 's -> CancellationToken -> Task<('s * 'e) option>) (state: 's) =
            Observable.Create(fun (obs : IObserver<_>) ->
                let cts = new CancellationTokenSource()
                let ct = cts.Token
                task {
                    let mutable innerState = state
                    let mutable isFinished = false
                    try
                        try
                            while not ct.IsCancellationRequested || not isFinished do
                                let! result = fn innerState ct
                                match result with
                                | Some (newState, output) ->
                                    innerState <- newState
                                    output |> obs.OnNext
                                | None ->
                                    isFinished <- true
                            obs.OnCompleted()
                        with e ->
                            obs.OnError e
                    finally
                        cts.Dispose()
                } |> ignore

                new Disposables.CancellationDisposable(cts) :> IDisposable
            )
        let flattenArray (observable : IObservable<array<_>>) =
            observable.SelectMany (fun x -> x :> seq<_>)

    let pollStreamForever (redisdb : IDatabase) (streamName : RedisKey) (startingPosition : RedisValue) (pollOptions : PollOptions) =
        Observable.taskUnfold (fun (nextPosition, pollDelay) ct -> task {
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
        |> Observable.flattenArray

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

        Observable.taskUnfold(fun nextPosition ct -> task {
            let! (response : StreamEntry []) = readStream nextPosition
            match response with
            | EmptyArray ->
                return None
            | entries ->
                let lastEntry = Seq.last entries
                let nextPosition = calculateNextPosition lastEntry.Id
                return Some (nextPosition, entries)
        }) startingPosition
        |> Observable.flattenArray


