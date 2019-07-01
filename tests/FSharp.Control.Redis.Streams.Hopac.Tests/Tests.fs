module Tests

open System
open Expecto
open StackExchange.Redis
open Hopac
open FSharp.Control.Redis.Streams.Core
open FSharp.Control.Redis.Streams.Hopac

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
        |> Stream.mapFun(fun x -> x |> printer ; x)//Debugging
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

    testCaseAsync "Stream should generate 20000 events" <| async {
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

        do! expecter.Await "Should have 20000 results" (TimeSpan.FromSeconds(30.)) |> Job.toAsync
    }


    testCaseAsync "Stream should generate large fields" <| async {
        let total = 200
        let db = redis.GetDatabase()
        let key = getUniqueKey "stream" "Foo"
        let expecter = StreamExpect<_>(fun s -> s |> Seq.length = total)
        pollStreamForever db key StreamPosition.Beginning PollOptions.Default
        |> expecter.CaptureFromStream

        job {
            do!
                [0..total]
                |> Seq.map(fun i ->
                    let data = ranStr (20000)
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
