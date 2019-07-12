namespace  FSharp.Control.Redis.Streams.Tests

module Core =
    open Expecto
    open FSharp.Control.Redis.Streams.Core

    [<Tests>]
    let coreTests =
        testList "Core tests" [
            test "EmptyArray matches empty array" {
                match Array.empty with
                | EmptyArray -> ()
                | xs -> failtestf "Should be empty array"
            }

            test "EmptyArray doesnt match filled array" {
                match [|1;2;3|] with
                | EmptyArray -> failtestf "Should be filled array"
                | xs -> ()
            }

            test "Int64 matches int64" {
                match "123" with
                | UInt64 i -> ()
                | _ -> failtestf "Should be int64"
            }

            test "Int64 doesn't match float" {
                match "123.456" with
                | UInt64 i -> failtestf "Should be int64"
                | _ -> ()
            }
        ]
