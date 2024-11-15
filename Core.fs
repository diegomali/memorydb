[<AutoOpen>]
module Core

open System
open System.Collections.Generic

type Dbtypes = 
        |Int of Int32
        | Float of float32
        | String of string
        | Map of Map<string, string>

type Key = string

type Result<'a> = 
    | Success of 'a
    | Error of string


type InMemoryDataBase = Dictionary<Key, Dbtypes>

let incr x =
    match x with
        | Int i -> Int (i + 1)
        | Float f -> Float (f + (float32 1.0))
        | _ -> failwith "Not a number"
