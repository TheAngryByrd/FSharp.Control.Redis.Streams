module Tests

open System
open Expecto
open Redis.Streams
open StackExchange.Redis
open Hopac

let getUniqueKey (keyType : string) (key : string) =
    let suffix = Guid.NewGuid().ToString()
    sprintf "%s:%s:%s" keyType key suffix
    |> RedisKey.op_Implicit

let printNameEntryValues (nve : NameValueEntry) =
    printfn "Key: %A - Value: %A" nve.Name nve.Value
let printStreamEntry (se : StreamEntry) =
    printfn "Id: %A" se.Id
    se.Values |> Seq.iter(printNameEntryValues )
let printRedisStream (rs : RedisStream) =
    printfn "key: %A" rs.Key
    rs.Entries |> Seq.iter(printStreamEntry)

let rkey (s : string) = RedisKey.op_Implicit s
let rval (s : string) = RedisValue.op_Implicit s
let nve name value = NameValueEntry(rval name, rval value)

let sp key position =
    StreamPosition(key, position)

//special keys

//This special ID means that XREAD should use as last ID the maximum ID already stored in the stream mystream, so that we will receive only new messages, starting from the time we started listening. This is similar to the tail -f Unix command in some way.
let dollar = "$"
// Beginning
let dash = "-"
// End
let plus = "+"

let (|EmptySeq|_|) xs =
    if xs |> Seq.isEmpty then Some EmptySeq
    else None

let (|Int64|_|) (str: string) =
    match Int64.TryParse(str) with
    | (true,v) -> Some v
    | _ -> None

type EntryId = {
    MillisecondsTime : int64
    SequenceNumber : int64
}
    with
        member x.Unparse () =
            sprintf "%d-%d" x.MillisecondsTime x.SequenceNumber

        member x.ToRedisValue () =
            x.Unparse() |> RedisValue.op_Implicit

        member x.IncrementSequence () =
            {x with SequenceNumber = x.SequenceNumber + 1L}

        static member Parse(s : string) =
            match s.Split('-') |> Array.toList with
            | [Int64 ms; Int64 sn] -> {MillisecondsTime = ms; SequenceNumber = sn }
            | [Int64 ms] ->{MillisecondsTime = ms; SequenceNumber = 0L }
            | _ -> failwithf "invalid EntryId format, should look like 0-0 but was %s" s

        static member Parse(s : RedisValue) =
            s |> string |> EntryId.Parse


type PollOptions = {
    MaxPollDelay : TimeSpan
    MaxPollDelayBuckets : float
    CountToPullATime : int option
}
    with
        static member Default = {
            MaxPollDelay = TimeSpan.FromMilliseconds(100.)
            MaxPollDelayBuckets = 10.
            CountToPullATime = Some 1000
        }

let inline minTimeSpan (t1 : TimeSpan) (t2 : TimeSpan) =
    Math.Min(t1.Ticks, t2.Ticks)
    |> TimeSpan.FromTicks

let pollStreamForever (redisdb : IDatabase) (streamName : RedisKey) (startingPosition : RedisValue) (pollOptions : PollOptions)  =
    let incPos (rv : RedisValue) = EntryId.Parse(rv).IncrementSequence().ToRedisValue()
    Stream.unfoldJob(fun (nextPosition, shouldDelay, nextDelayPeriod) -> job {
        if shouldDelay then
            do! timeOut pollOptions.MaxPollDelay
        let! (response : StreamEntry []) = redisdb.StreamRangeAsync(streamName, minId = Nullable(nextPosition), count = (pollOptions.CountToPullATime |> Option.toNullable))
        match response with
        | EmptySeq ->
            let increment = (float pollOptions.MaxPollDelay.Ticks / pollOptions.MaxPollDelayBuckets)
            let nextPollDelay = nextDelayPeriod + TimeSpan.FromTicks(int64 increment)
            let nextPollDelay = minTimeSpan nextPollDelay pollOptions.MaxPollDelay
            return Some (Array.empty, (nextPosition, true, nextPollDelay ))
        | entries ->
            let lastEntry = entries |> Seq.last
            let nextPosition = lastEntry.Id |> incPos
            return Some (entries, (nextPosition, false, TimeSpan.FromMilliseconds(0.) ))
    }) (startingPosition, false, TimeSpan.FromMilliseconds(0.))
    |> Stream.appendMap (Stream.ofSeq)

let pollStreamUntil (redisdb : IDatabase) (streamName : RedisKey) (startingPosition : RedisValue) (pollOptions : PollOptions) (until : Alt<_>) =
    pollStreamForever redisdb streamName startingPosition pollOptions
    |> Stream.takeUntil until


let failDueToTimeout message (ts : TimeSpan) =
    failtestf "%s. Expected task to complete but failed after timeout of %f ms"  message ts.TotalMilliseconds

type StreamExpect<'a> (predicate : seq<'a> -> bool) =
    let values = ResizeArray<'a>()
    let predicateConditionMet = IVar<unit>()
    let mutable debugPrint : Option<'a -> unit> = None
    let checkPredicate () =
        if predicate values then
            IVar.tryFill predicateConditionMet () |> start

    member this.Values
        with get () = values


    member this.DebugPrint
        with get () = debugPrint
        and  set (v) = debugPrint <- v
    member this.CaptureFromStream (s : Stream<'a>) =
        let printer =
            match debugPrint with
            | Some d -> d
            | None -> ignore
        s
        //Debuging
        |> Stream.mapFun(fun x -> x |> printer ; x)
        |> Stream.iterFun(values.Add >> checkPredicate)
        |> start

    member this.Await message (timeout : TimeSpan) = job {

        do! Alt.choose [
            predicateConditionMet :> Alt<unit>
            timeOut timeout |> Alt.afterFun (fun _ -> failDueToTimeout message timeout)
        ]
    }

let ranStr n : string =
    let r = Random()
    String(Array.init n (fun _ -> char (r.Next(97,123))))



let options = ConfigurationOptions.Parse("localhost", ResponseTimeout = 100000, SyncTimeout=100000, ConnectTimeout=100000)
let redis = ConnectionMultiplexer.Connect(options)
[<Tests>]
let tests =
    testSequenced <|
    testList "samples" [
    testCaseAsync "Stream should generate 2 events" <| async {
        let db = redis.GetDatabase()
        let key = getUniqueKey "stream" "Foo"
        let expecter = StreamExpect<_>(fun s -> s |> Seq.length = 2)
        pollStreamForever db key StreamPosition.Beginning PollOptions.Default
        |> expecter.CaptureFromStream

        let values =
            [|
                nve "Field1" "Value1"
                nve "Field2" "Value3"
            |]
        let! x = db.StreamAddAsync(key, values) |> Async.AwaitTask
        let values =
            [|
                nve "Field1" "Value4"
                nve "Field2" "Value6"
            |]
        let! x = db.StreamAddAsync(key, values) |> Async.AwaitTask
        do! expecter.Await "Should have 2 results" (TimeSpan.FromSeconds(1.)) |> Job.toAsync
    }

    testCaseAsync "Stream should generate 200000 events" <| async {
        let total = 200000
        let db = redis.GetDatabase()
        let key = getUniqueKey "stream" "Foo"
        let expecter = StreamExpect<_>(fun s -> s |> Seq.length = total)
        pollStreamForever db key StreamPosition.Beginning PollOptions.Default
        |> expecter.CaptureFromStream

        job {
            do!
                [0..total]
                |> Seq.map(fun i ->
                    let values =
                        [|
                            NameValueEntry (RedisValue.op_Implicit "Field1", RedisValue.op_Implicit total)
                        |]
                    job {
                        let! x = db.StreamAddAsync(key, values) |> Async.AwaitTask
                        return ()
                    })
                |> Stream.ofSeq
                |> Stream.mapPipelinedJob (Environment.ProcessorCount * 4096 * 2) id
                |> Stream.iter
        } |> start

        do! expecter.Await "Should have 200000 results" (TimeSpan.FromSeconds(30.)) |> Job.toAsync
    }


    testCaseAsync "Stream should generate large fields" <| async {
        let total = 20000
        let db = redis.GetDatabase()
        let key = getUniqueKey "stream" "Foo"
        let expecter = StreamExpect<_>(fun s -> s |> Seq.length = total)
        pollStreamForever db key StreamPosition.Beginning PollOptions.Default
        |> expecter.CaptureFromStream

        job {
            do!
                [0..total]
                |> Seq.map(fun i ->
                    let data = ranStr (2000000)
                    let values =
                        [|
                            NameValueEntry (RedisValue.op_Implicit "Field1", RedisValue.op_Implicit data)
                        |]
                    job {
                        let! x = db.StreamAddAsync(key, values) |> Async.AwaitTask
                        return ()
                    })
                |> Stream.ofSeq
                |> Stream.mapPipelinedJob (Environment.ProcessorCount * 2) id
                |> Stream.iter
        } |> start

        do! expecter.Await "Should have 2 results" (TimeSpan.FromSeconds(300.)) |> Job.toAsync
    }
  ]
