module Tests

open System
open Expecto
open StackExchange.Redis
open System.Threading
open System.Threading.Tasks
open Akka.Streams.Dsl
open FSharp.Control.Redis.Streams.Core
open FSharp.Control.Redis.Streams.Akka
open FSharp.Control.Tasks.V2.ContextInsensitive
open Hopac
open Akka.Actor
open Akka.Streams

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

let inline map (fn: 't -> 'u) (source) : Source<'u, 'mat> =
    SourceOperations.Select(source, Func<_, _>(fn))

let inline runForEach (mat: #IMaterializer) (fn: 't -> unit) (source: Source<'t, 'mat>) = task {
    do! source.RunForeach(Action<_>(fn), mat)
}

let failDueToTimeout message (ts : TimeSpan) =
    failtestf "%s. Expected task to complete but failed after timeout of %f ms"  message ts.TotalMilliseconds

type StreamExpect<'a> (system : ActorSystem, predicate : seq<'a> -> bool) =
    let mat = system.Materializer()
    let values = ResizeArray<'a>()
    let cts = new CancellationTokenSource()
    let predicateConditionMet = TaskCompletionSource<unit>()
    let mutable debugPrint : Option<'a -> unit> = None
    // let mutable subscription : IDisposable = new Disposables.BooleanDisposable() :> IDisposable
    let checkPredicate () =
        if predicate values then
            predicateConditionMet.TrySetResult ()
            |> ignore

    member this.Values
        with get () = values

    member this.DebugPrint
        with get () = debugPrint
        and  set (v) = debugPrint <- v
    member this.CaptureFromStream (s : Source<_,_>) =
        let printer =
            match debugPrint with
            | Some d -> d
            | None -> ignore

        s
        |> map (fun x -> x |> printer; x)
        |> runForEach mat (values.Add >> checkPredicate)
        |> ignore


    member this.Await message (timeout : TimeSpan) = task {
        let generateTimeout () = task {
            do! Task.Delay(timeout,cts.Token)
            failDueToTimeout message timeout
        }

        let! firstFinishedTask = Task.WhenAny [
            predicateConditionMet.Task
            generateTimeout ()
        ]
        cts.Cancel()
        do! firstFinishedTask
    }

    interface IDisposable with
        member x.Dispose() =
            mat.Dispose()
            cts.Dispose()

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
        use system = ActorSystem.Create("system")
        let db = redis.GetDatabase()
        let key = getUniqueKey "stream" "Foo"
        use expecter = new StreamExpect<_>(system, fun s -> s |> Seq.length = 2)
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
        do! expecter.Await "Should have 2 results" (TimeSpan.FromSeconds(1.)) |> Async.AwaitTask
    }

    testCaseAsync "Stream should generate 20000 events" <| async {
        use system = ActorSystem.Create("system")
        let total = 20000
        let db = redis.GetDatabase()
        let key = getUniqueKey "stream" "Foo"
        use expecter = new StreamExpect<_>(system, fun s -> s |> Seq.length = total)
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

        do! expecter.Await "Should have 20000 results" (TimeSpan.FromSeconds(30.)) |> Async.AwaitTask
    }


    testCaseAsync "Stream should generate large fields" <| async {
        use system = ActorSystem.Create("system")
        let total = 200
        let db = redis.GetDatabase()
        let key = getUniqueKey "stream" "Foo"
        use expecter = new StreamExpect<_>(system, fun s -> s |> Seq.length = total)
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

        do! expecter.Await "Should have 2 results" (TimeSpan.FromSeconds(30.)) |> Async.AwaitTask
    }
  ]