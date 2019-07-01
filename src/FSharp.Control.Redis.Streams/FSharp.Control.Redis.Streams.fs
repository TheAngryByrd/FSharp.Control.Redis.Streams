namespace FSharp.Control.Redis.Streams

open System
open System.Runtime.CompilerServices

[<Extension>]
type TimeSpan () =
    [<Extension>]
    static member Min (t1 : System.TimeSpan) (t2 : System.TimeSpan) =
        Math.Min(t1.Ticks, t2.Ticks)
        |> TimeSpan.FromTicks

module Core =
    open System
    open StackExchange.Redis

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
                | [Int64 ms; Int64 sn] -> { MillisecondsTime = ms; SequenceNumber = sn }
                | [Int64 ms] ->{ MillisecondsTime = ms; SequenceNumber = 0L }
                | _ -> failwithf "invalid EntryId format, should look like 0-0 but was %s" s

            static member Parse(rv : RedisValue) =
                rv |> string |> EntryId.Parse

            static member CalculateNextPosition (rv : RedisValue) =
                EntryId.Parse(rv).IncrementSequence().ToRedisValue()

    type PollOptions = {
        MaxPollDelay : TimeSpan
        MaxPollDelayBuckets : float
        CountToPullATime : int option
        CommandFlags : CommandFlags
    }
        with
            static member Default = {
                MaxPollDelay = TimeSpan.FromMilliseconds(100.)
                MaxPollDelayBuckets = 10.
                CountToPullATime = Some 1000
                CommandFlags = CommandFlags.None
            }
