[<AutoOpen>]
module Types
open System
open System.Collections.Generic

type Dbtypes = 
        | Int of Int32
        | Float of float32
        | String of string
        | Map of Map<string, string>
        | None

type Key = string

type Result<'a> = 
    | Success of 'a
    | Error of string


type InMemoryDataBase = Dictionary<Key, Dbtypes>
type Command = (unit -> Result<Dbtypes>)  * AsyncReplyChannel<Result<Dbtypes>>

let incr x =
    match x with
        | Int i -> Int (i + 1)
        | Float f -> Float (f + (float32 1.0))
        | _ -> failwith "Not a number"

let error e =
    Error e
    
let bind f x = async {
    let! a = x
    return f x
}

let inline (>>=) xAsync f  = async {
        let! x = xAsync
        return match x with
                    | Success s -> f s
                    | Error e -> Error e
    }
        
let inline value (x: Async<Result<'a>>) = async {
    let! value = x
    return match value with
            | Success s -> s
            | _ -> Unchecked.defaultof<'a> 
}

let NotFound = "Not Found"
let OK = "OK"
let successInt = (Dbtypes.Int >> Success)
let successOk = String OK |> Success

       
