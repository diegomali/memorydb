module InMemoryDB

open System
open Newtonsoft.Json
open Types

type DbInstance = {
    GET:  Key -> Async<Result<Dbtypes>>
    SET: Key -> Dbtypes -> Async<Result<Dbtypes>>
    SETWithEX: Key -> Dbtypes -> int -> Async<Result<Dbtypes>>
    DBSIZE: Async<Result<Dbtypes>>
    DEL: Key array -> Async<Result<Dbtypes>>
    INCR: Key -> Async<Result<Dbtypes>>
    ZCARD: Key -> Async<Result<Dbtypes>>
    ZADD: Key -> string -> string -> Async<Result<Dbtypes>>
    ZRANK: Key -> string -> Async<Result<Dbtypes>>
    ZRANGE: Key -> int -> int -> Async<Result<Dbtypes>>
}

let zero = 0 |> successInt
let DEL (inMemoryDataBase: InMemoryDataBase) (keys : Key array) = 
    keys
        |> Array.fold (fun s key ->
            match inMemoryDataBase.ContainsKey key with
            | true ->
                match (inMemoryDataBase.Remove key) with
                | true -> s + 1
                | _ -> 0
            | _ -> 0) 0
        |> successInt

let SET_BASE (inMemoryDataBase: InMemoryDataBase) (key: Key) (value: Dbtypes) =
    match inMemoryDataBase.ContainsKey key with
    | true -> 
        inMemoryDataBase.[key] <- value
        successOk
    | _ -> 
        inMemoryDataBase.Add(key, value)
        successOk


let SET = SET_BASE

let GET (inMemoryDataBase: InMemoryDataBase) (key: Key) =
    match inMemoryDataBase.ContainsKey key with
    | true -> Success inMemoryDataBase.[key]
    | _ -> Error NotFound


let DBSIZE (inMemoryDataBase: InMemoryDataBase) = inMemoryDataBase.Count |> successInt

let ZCARD (inMemoryDataBase: InMemoryDataBase) key =
    match inMemoryDataBase.ContainsKey key with
    | true -> 
        match inMemoryDataBase.[key] with
        | Map s -> s
                |> Map.count
                |> successInt
        | _ -> zero
    | _ -> zero


let ZADD (inMemoryDataBase: InMemoryDataBase) (key: Key) score member' =
    match inMemoryDataBase.ContainsKey key with
        | true -> 
            match inMemoryDataBase.[key] with
                | Map sortedSet ->
                    inMemoryDataBase.[key] <- sortedSet
                        |> Map.filter (fun _ i -> i <> member')
                        |> Map.add score member'
                        |> Map
                    zero
                | _ -> zero
        | _ ->
            let sortedSet = Map.empty<string, string>
                            |> Map.add score  member'
            inMemoryDataBase.Add(key, (Map sortedSet))
            1 |> successInt


let ZRANK (inMemoryDataBase: InMemoryDataBase) key member' =
    match inMemoryDataBase.ContainsKey key with
    | true -> 
        match inMemoryDataBase.[key] with
        | Map sortedSet ->
                sortedSet       
                |> Map.toArray
                |> Array.tryFindIndex (fun (_, x)-> x = member')
                |> (fun x ->
                    match x with
                        | Some i -> successInt i
                        | Option.None -> error NotFound
                    )
        | _ -> error NotFound
    | _ -> error NotFound
let ZRANGE (inMemoryDataBase: InMemoryDataBase) key start stop = 
    let (&&&) size x = if x < 0 then size + x else x
    let getSeq size start stop = [| for i in size &&& start .. size &&& stop -> i |]
    let trim (s: String) = s.Trim()

    match inMemoryDataBase.ContainsKey key with
            | true -> 
                match inMemoryDataBase.[key] with
                | Map sortedSet ->
                    let size = sortedSet.Count
                    let range = getSeq size start stop 
                    sortedSet       
                        |> Map.toArray
                        |> Array.mapi (fun i (_, x)  -> i, x )
                        |> Array.filter (fun (i, x) -> range |> Array.contains i )
                        |> Array.mapi(fun i (_, x) -> (sprintf ":%d %s" (i + 1) x))
                        |> (String.concat ", " >> trim)
                        |> String
                        |> Success 
                | _ -> error NotFound
            | _ -> error NotFound

let createAgent () =
    MailboxProcessor<Command>.Start(fun inbox -> 
        let rec loop () = async {
            let! (command, replyChanel) = inbox.Receive()
            command() |> replyChanel.Reply
            return! loop()
        }
        loop()
    )

let timer (interval:int)  scheduledAction = async {
    do! interval |> Async.Sleep
    scheduledAction()
}

let parseCommand (cmd: string) =
    let a = cmd.Split [|' '|]
    try
        Success (JsonConvert.DeserializeObject cmd)
    with ex ->
        printfn "%A" ex
        Error "Invalid argument"
    

let startDatabase () =
    let db = new InMemoryDataBase()
    let agent = createAgent()

    let SET' key value =
        agent
            .PostAndAsyncReply(fun replyChannel -> (fun () -> SET db key value), replyChannel)

    let GET' key = 
        agent
            .PostAndAsyncReply(fun replyChannel -> (fun () -> GET db key), replyChannel) 

    let DEL' (keys: Key array) =
        agent
            .PostAndAsyncReply(fun replyChannel -> (fun () -> (DEL db keys)), replyChannel)

    let SetWithEx key value ex =
        async {
            let! result = SET' key value
            timer ex (fun _ -> DEL' [|key|] |> ignore) |> Async.Start
            return result
        }

    let DBSIZE' =
        agent
            .PostAndAsyncReply(fun replyChanel -> (fun () -> DBSIZE db), replyChanel)

    let ZCARD' key =
        agent
            .PostAndAsyncReply(fun replyChanel -> (fun () -> ZCARD db key), replyChanel)

    let ZADD' key score member' =
        agent
            .PostAndAsyncReply(fun replyChanel -> (fun () -> ZADD db key score member'), replyChanel)

    let ZRANK' key member' =
        agent
            .PostAndAsyncReply(fun replyChanel -> (fun () -> ZRANK db key member'), replyChanel)

    let ZRANGE' key start stop =
        agent
            .PostAndAsyncReply(fun replyChanel -> (fun () -> ZRANGE db key start stop), replyChanel)

    let INCR key =
        let errMSG = Error "Not a number"
        async {
            match! GET' key with
            | Success value ->
                let newState = incr value
                return! SET' key newState
            | Error e -> 
                return Error e
        }


    {
        DbInstance.GET = GET'
        SET = SET'
        SETWithEX = SetWithEx
        DBSIZE = DBSIZE'
        DEL = DEL'
        INCR = INCR
        ZADD = ZADD'
        ZCARD = ZCARD'
        ZRANK = ZRANK'
        ZRANGE = ZRANGE'
    }
