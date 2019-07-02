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


    let pollStreamForever (redisdb : IDatabase) (streamName : RedisKey) (startingPosition : RedisValue) (pollOptions : PollOptions) =
        let calculateNextPollDelay (nextPollDelay) =
            let increment = (float pollOptions.MaxPollDelay.Ticks / pollOptions.MaxPollDelayBuckets)
            let nextPollDelay = nextPollDelay + TimeSpan.FromTicks(int64 increment)
            TimeSpan.Min nextPollDelay pollOptions.MaxPollDelay

        Observable.Create(fun (obs : IObserver<_>) ->
            let cts = new CancellationTokenSource()
            let ct = cts.Token
            task {
                let mutable nextPollDelay = TimeSpan.Zero
                let mutable nextPosition = startingPosition
                try
                    try
                        while not ct.IsCancellationRequested do
                            let! (response : StreamEntry []) = redisdb.StreamRangeAsync(streamName, minId = Nullable(nextPosition), count = (Option.toNullable pollOptions.CountToPullATime))
                            match response with
                            | EmptySeq ->
                                nextPollDelay <- calculateNextPollDelay nextPollDelay
                                do! Task.Delay(nextPollDelay, ct)
                            | entries ->
                                let lastEntry = Seq.last entries
                                nextPosition <- EntryId.CalculateNextPosition lastEntry.Id
                                nextPollDelay <- TimeSpan.Zero
                                entries |> Array.iter obs.OnNext
                        obs.OnCompleted()
                    with e ->
                        obs.OnError e
                finally
                    cts.Dispose()
            } |> ignore

            new Disposables.CancellationDisposable(cts) :> IDisposable
        )


