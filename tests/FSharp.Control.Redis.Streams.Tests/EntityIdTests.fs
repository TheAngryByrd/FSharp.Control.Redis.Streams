module Tests

open System
open Expecto
open StackExchange.Redis
open FSharp.Control.Redis.Streams.Core

[<Tests>]
let coreTests =
    testList "EntityId tests" [
        test "Parse parses millisecond and sequence" {
            let milliseconds = 100000L
            let sequenceId = 4L
            let expectedEntryId = {
                MillisecondsTime = milliseconds
                SequenceNumber =sequenceId
            }
            let entryIdStr = (sprintf "%d-%d" milliseconds sequenceId)
            let actualEntryId = EntryId.Parse entryIdStr


            Expect.equal actualEntryId expectedEntryId "Should parse EntryId"
        }

        test "Parse parses millisecond" {
            let milliseconds = 100000L
            let expectedEntryId = {
                MillisecondsTime = milliseconds
                SequenceNumber = 0L
            }
            let entryIdStr = (sprintf "%d" milliseconds )
            let actualEntryId = EntryId.Parse entryIdStr

            Expect.equal actualEntryId expectedEntryId "Should parse EntryId"
        }

        test "CalculateNextPosition" {
            let milliseconds = 100000L
            let currentEntry = {
                MillisecondsTime = milliseconds
                SequenceNumber = 0L
            }
            let expectedEntryId = RedisValue.op_Implicit (sprintf "%d-1" milliseconds)
            let actualEntryId = currentEntry.IncrementSequence().ToRedisValue()

            Expect.equal actualEntryId expectedEntryId "Should parse EntryId"
        }
    ]
