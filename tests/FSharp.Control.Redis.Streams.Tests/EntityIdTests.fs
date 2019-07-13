module Tests

open System
open Expecto
open StackExchange.Redis
open FSharp.Control.Redis.Streams.Core

[<Tests>]
let coreTests =
    testList "EntityId tests" [
        test "Parse parses millisecond and sequence" {
            let milliseconds = 100000UL
            let sequenceId = 4UL
            let expectedEntryId = {
                MillisecondsTime = milliseconds
                SequenceNumber =sequenceId
            }
            let entryIdStr = (sprintf "%d-%d" milliseconds sequenceId)
            let actualEntryId = EntryId.Parse entryIdStr


            Expect.equal actualEntryId expectedEntryId "Should parse EntryId"
        }

        test "Parse parses millisecond" {
            let milliseconds = 100000UL
            let expectedEntryId = {
                MillisecondsTime = milliseconds
                SequenceNumber = 0UL
            }
            let entryIdStr = (sprintf "%d" milliseconds )
            let actualEntryId = EntryId.Parse entryIdStr

            Expect.equal actualEntryId expectedEntryId "Should parse EntryId"
        }

        test "IncrementSequence when SequenceNumber <> UInt64.Max" {
            let milliseconds = 100000UL
            let currentEntry = {
                MillisecondsTime = milliseconds
                SequenceNumber = 0UL
            }
            let expectedEntryId = RedisValue.op_Implicit (sprintf "%d-1" milliseconds)
            let actualEntryId = currentEntry.IncrementSequence().ToRedisValue()

            Expect.equal actualEntryId expectedEntryId "Should parse EntryId"
        }


        test "DecrementSequence when SequenceNumber > 1" {
            let milliseconds = 100000UL
            let currentEntry = {
                MillisecondsTime = milliseconds
                SequenceNumber = 1UL
            }
            let expectedEntryId = RedisValue.op_Implicit (sprintf "%d-0" milliseconds)
            let actualEntryId = currentEntry.DecrementSequence().ToRedisValue()

            Expect.equal actualEntryId expectedEntryId "Should parse EntryId"
        }

        test "DecrementSequence when SequenceNumber = 0" {
            let milliseconds = 100000UL
            let currentEntry = {
                MillisecondsTime = milliseconds
                SequenceNumber = 0UL
            }
            let expectedEntryId = RedisValue.op_Implicit (sprintf "%d-%d" (milliseconds - 1UL) UInt64.MaxValue)
            let actualEntryId = currentEntry.DecrementSequence().ToRedisValue()

            Expect.equal actualEntryId expectedEntryId "Should parse EntryId"
        }
    ]
