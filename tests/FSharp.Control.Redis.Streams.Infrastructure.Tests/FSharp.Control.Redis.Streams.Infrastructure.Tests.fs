namespace FSharp.Control.Redis.Streams.Infrastructure.Tests
open StackExchange.Redis
open System
open Hopac

[<AutoOpen>]
module Foo =
    let getUniqueKey (keyType : string) (key : string) =
        let suffix = Guid.NewGuid().ToString()
        sprintf "%s:%s:%s" keyType key suffix
        |> RedisKey.op_Implicit

    let getUniqueStreamKey (key : string) =
        getUniqueKey "stream" key

    // No way to delete a stream, so setting the max size to 1.  It's close enough for now.
    let disposableStream (db : IDatabase) (streamName : RedisKey) = {
        new IDisposable with
            member x.Dispose () = db.StreamTrim(streamName,1) |> ignore
    }

    let ranStr n : string =
        let r = Random()
        String(Array.init n (fun _ -> char (r.Next(97,123))))

    let generateData count size =
        [1..count]
        |> List.map(fun i ->
            let data = ranStr size
            [|
                NameValueEntry (RedisValue.op_Implicit "Field1", RedisValue.op_Implicit data)
            |]
        )

    let generateDataForStreamSeq (db : IDatabase) (streamName) (count) size = job {
        let data = generateData count size
        let! keys =
            data
            |> Seq.map(fun values -> job{
                let! id = db.StreamAddAsync(streamName, values)
                return (id, values)
            })
            |> Job.seqCollect
        return keys
    }

    let generateDataForStreamCon (db : IDatabase) (streamName) (count) size = job {
        let data = generateData count size
        let! keys =
            data
            |> Seq.map(fun values -> job{
                let! id = db.StreamAddAsync(streamName, values)
                return (id, values)
            })
            |> Job.conCollect
            |> Job.map (Seq.sortBy(fun (id,_) -> id)) // To guarentee order matches inserted
        return keys
    }
