module Tests

open System
open Expecto
open StackExchange.Redis
open Hopac
open FSharp.Control.Redis.Streams
open FSharp.Control.Redis.Streams.Core
open FSharp.Control.Redis.Streams.Hopac
open FSharp.Control.Redis.Streams.Infrastructure.Tests



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
        |> Stream.mapFun(fun x -> x |> printer ; x) //Debugging
        |> Stream.iterFun(values.Add >> checkPredicate)
        |> start

    member this.Await message (timeout : TimeSpan) = job {

        do! Alt.choose [
            predicateConditionMet :> Alt<unit>
            timeOut timeout |> Alt.afterFun (fun _ -> failDueToTimeout message timeout)
        ]
    }



let options = ConfigurationOptions.Parse("localhost", ResponseTimeout = 100000, SyncTimeout=100000, ConnectTimeout=100000)
let redis = ConnectionMultiplexer.Connect(options)
[<Tests>]
let pollStreamForeverTests =
    // testSequenced <|
    testList "pollStreamForever" [
    testCaseJob "Stream should generate 2 events" <| job {
        let db = redis.GetDatabase()
        let streamName = getUniqueStreamKey "StreamGenerate2Events"
        use _ = disposableStream db streamName
        let expecter = StreamExpect<_>(fun s -> s |> Seq.length = 2)

        pollStreamForever db streamName StreamPosition.Beginning PollOptions.Default
        |> expecter.CaptureFromStream

        let values =
            [|
                nve "Field1" "Value1"
                nve "Field2" "Value3"
            |]
        let! x = db.StreamAddAsync(streamName, values)
        let values =
            [|
                nve "Field1" "Value4"
                nve "Field2" "Value6"
            |]
        let! x = db.StreamAddAsync(streamName, values)

        do! expecter.Await "Should have 2 results" (TimeSpan.FromSeconds(1.))
    }

    testCaseJob "Stream should generate large number of events" <| job {
        let numberOfEvents = 20000
        let db = redis.GetDatabase()
        let streamName = getUniqueStreamKey  "StreamGenerate20000Events"

        use _ = disposableStream db streamName
        let expecter = StreamExpect<_>(fun s -> s |> Seq.length = numberOfEvents)
        pollStreamForever db streamName StreamPosition.Beginning PollOptions.Default
        |> expecter.CaptureFromStream

        generateDataForStreamCon db streamName numberOfEvents 200
        |> Job.Ignore
        |> start


        do! expecter.Await (sprintf "Should have %d results" numberOfEvents) (TimeSpan.FromSeconds(30.))
    }


    testCaseJob "Stream should generate large fields" <| job {
        let numberOfEvents = 200
        let db = redis.GetDatabase()
        let streamName = getUniqueStreamKey  "StreamGenerateLargeField"
        use _ = disposableStream db streamName
        let expecter = StreamExpect<_>(fun s -> s |> Seq.length = numberOfEvents)
        pollStreamForever db streamName StreamPosition.Beginning PollOptions.Default
        |> expecter.CaptureFromStream

        generateDataForStreamCon db streamName numberOfEvents 20000
        |> Job.Ignore
        |> start

        do! expecter.Await "Should have 200 results" (TimeSpan.FromSeconds(30.))
    }

]



[<Tests>]
let readFromStreamTests =
    // testSequenced <|
    testList "readFromStream" [
        testCaseJob "Read forward all Ascending" <| job {
            let numberOfEvents = 10
            let db = redis.GetDatabase()
            let streamName = getUniqueStreamKey "ReadForwardAll"
            use _ = disposableStream db streamName
            let! data = generateDataForStreamCon db streamName numberOfEvents 200

            let expecter = StreamExpect<_>(fun s -> s |> Seq.length = numberOfEvents)

            ReadStreamConfig.fromStreamName streamName
            |> readFromStream db
            |> expecter.CaptureFromStream

            do! expecter.Await (sprintf "Should have %d results" numberOfEvents) (TimeSpan.FromSeconds(10.))
            let actualValues =
                expecter.Values
                |> Seq.collect(fun v -> v.Values)

            let expected =
                data
                |> Seq.map snd
                |> Seq.collect id
                |> Seq.toList
            Expect.sequenceEqual actualValues expected "Should be same order"
        }

        testCaseJob "Read forward withCountToPull Ascending" <| job {
            let numberOfEvents = 10
            let db = redis.GetDatabase()
            let streamName = getUniqueStreamKey "ReadForwardwithCountToPull"
            use _ = disposableStream db streamName
            let! data= generateDataForStreamCon db streamName numberOfEvents 200

            let expecter = StreamExpect<_>(fun s -> s |> Seq.length = numberOfEvents)

            ReadStreamConfig.fromStreamName streamName
            |> ReadStreamConfig.withCountToPullATime 3
            |> readFromStream db
            |> expecter.CaptureFromStream

            do! expecter.Await (sprintf "Should have %d results" numberOfEvents) (TimeSpan.FromSeconds(10.))
            let actualValues =
                expecter.Values
                |> Seq.collect(fun v -> v.Values)

            let expected =
                data
                |> Seq.map snd
                |> Seq.collect id
                |> Seq.toList
            Expect.sequenceEqual actualValues expected "Should be same order"
        }

        testCaseJob "Read backward all" <| job {
            let numberOfEvents = 10
            let db = redis.GetDatabase()
            let streamName = getUniqueStreamKey "ReadbackwardAll"
            use _ = disposableStream db streamName
            let! data= generateDataForStreamCon db streamName numberOfEvents 200

            let expecter = StreamExpect<_>(fun s -> s |> Seq.length = numberOfEvents)

            ReadStreamConfig.fromStreamName streamName
            |> ReadStreamConfig.withDescending
            |> readFromStream db
            |> expecter.CaptureFromStream

            do! expecter.Await (sprintf "Should have %d results" numberOfEvents) (TimeSpan.FromSeconds(10.))
            let actualValues =
                expecter.Values
                |> Seq.collect(fun v -> v.Values)
                |> Seq.toList
            let expected =
                data
                |> Seq.map snd
                |> Seq.collect id
                |> Seq.rev
                |> Seq.toList
            Expect.sequenceEqual actualValues expected  "Should be same order"
        }

        testCaseJob "Read backward count" <| job {
            let numberOfEvents = 10
            let db = redis.GetDatabase()
            let streamName = getUniqueStreamKey "Readbackwardcount"
            use _ = disposableStream db streamName
            let! data= generateDataForStreamCon db streamName numberOfEvents 200

            let expecter = StreamExpect<_>(fun s -> s |> Seq.length = numberOfEvents)

            ReadStreamConfig.fromStreamName streamName
            |> ReadStreamConfig.withDescending
            |> ReadStreamConfig.withCountToPullATime 1
            |> readFromStream db
            |> expecter.CaptureFromStream

            do! expecter.Await (sprintf "Should have %d results" numberOfEvents) (TimeSpan.FromSeconds(10.))
            let actualValues =
                expecter.Values
                |> Seq.collect(fun v -> v.Values)
                |> Seq.toList
            let expected =
                data
                |> Seq.map snd
                |> Seq.collect id
                |> Seq.rev
                |> Seq.toList
            Expect.sequenceEqual actualValues expected  "Should be same order"
        }

    ]
