namespace FSharp.Control.Redis.Streams

open System
open StackExchange.Redis

type ReadStreamConfig = {
    StreamName : RedisKey
    MinId : RedisValue option
    MaxId : RedisValue option
    CountToPullATime : int option
    MessageOrder : Order
    Flags : CommandFlags
}

module ReadStreamConfig =

    let fromStreamName streamName = {
        StreamName = streamName
        MinId = None
        MaxId = None
        CountToPullATime = None
        MessageOrder = Order.Ascending
        Flags = CommandFlags.None
    }

    let withMinId minId readStream = {
        readStream with MinId = Some minId
    }

    let withMaxId maxId readStream = {
        readStream with MaxId = Some maxId
    }

    let withCountToPullATime count readStream = {
        readStream with CountToPullATime = Some count
    }

    let withAscending readStream = {
        readStream with MessageOrder = Order.Ascending
    }

    let withDescending readStream = {
        readStream with MessageOrder = Order.Descending
    }

    let withFlags flags readStream = {
        readStream with Flags = flags
    }

module StreamConstants =

    open StackExchange.Redis
    /// <summary>
    /// The "~" value used with the MAXLEN option.
    /// </summary>
    let ApproximateMaxLen = RedisValue.op_Implicit "~";

    /// <summary>
    /// The "*" value used with the XADD command.
    /// </summary>
    let AutoGeneratedId =  RedisValue.op_Implicit "*";

    /// <summary>
    /// The "$" value used in the XGROUP command. Indicates reading only new messages from the stream.
    /// </summary>
    let NewMessages = RedisValue.op_Implicit "$";

    /// <summary>
    /// The "0" value used in the XGROUP command. Indicates reading all messages from the stream.
    /// </summary>
    let AllMessages =  RedisValue.op_Implicit "0";

    /// <summary>
    /// The "-" value used in the XRANGE, XREAD, and XREADGROUP commands. Indicates the minimum message ID from the stream.
    /// </summary>
    let ReadMinValue =  RedisValue.op_Implicit "-";

    /// <summary>
    /// The "+" value used in the XRANGE, XREAD, and XREADGROUP commands. Indicates the maximum message ID from the stream.
    /// </summary>
    let ReadMaxValue = RedisValue.op_Implicit "+";

    /// <summary>
    /// The ">" value used in the XREADGROUP command. Use this to read messages that have not been delivered to a consumer group.
    /// </summary>
    let UndeliveredMessages =  RedisValue.op_Implicit ">";

module Core =
    open System
    open StackExchange.Redis


    /// **Description**
    /// Active pattern for Array.isEmpty
    ///
    let (|EmptyArray|_|) xs =
        if xs |> Array.isEmpty then Some EmptyArray
        else None

    let (|UInt64|_|) (str: string) =
        match UInt64.TryParse(str) with
        | (true,v) -> Some v
        | _ -> None
    [<Struct>]
    type EntryId = {
        MillisecondsTime : uint64
        SequenceNumber : uint64
    }
        with
            member x.Unparse () =
                sprintf "%d-%d" x.MillisecondsTime x.SequenceNumber

            member x.ToRedisValue () =
                x.Unparse() |> RedisValue.op_Implicit

            member x.Increment () =
                if x.SequenceNumber = UInt64.MaxValue then
                    {
                        MillisecondsTime = x.MillisecondsTime + 1UL
                        SequenceNumber = 0UL
                    }
                else
                    {
                        MillisecondsTime = x.MillisecondsTime
                        SequenceNumber = x.SequenceNumber + 1UL
                    }

            member x.Decrement () =
                if x.SequenceNumber = 0UL then
                    {
                        MillisecondsTime = x.MillisecondsTime - 1UL
                        SequenceNumber = UInt64.MaxValue
                    }
                else
                    {
                        MillisecondsTime = x.MillisecondsTime
                        SequenceNumber = x.SequenceNumber - 1UL
                    }

            static member Parse(s : string) =
                match s.Split('-') |> Array.toList with
                | [UInt64 ms; UInt64 sn] -> { MillisecondsTime = ms; SequenceNumber = sn }
                | [UInt64 ms] ->{ MillisecondsTime = ms; SequenceNumber = 0UL }
                | _ -> failwithf "invalid EntryId format, should look like 0-0 but was %s" s

            static member Parse(rv : RedisValue) =
                rv |> string |> EntryId.Parse

            static member CalculateNextPositionIncr (rv : RedisValue) =
                EntryId.Parse(rv).Increment().ToRedisValue()

            static member CalculateNextPositionDesc (rv : RedisValue) =
                EntryId.Parse(rv).Decrement().ToRedisValue()

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
            member self.CalculateNextPollDelay (currentPollDelay : TimeSpan) =
                let increment = (float self.MaxPollDelay.Ticks / self.MaxPollDelayBuckets)
                let nextPollDelay = currentPollDelay + TimeSpan.FromTicks(int64 increment)
                min nextPollDelay self.MaxPollDelay
