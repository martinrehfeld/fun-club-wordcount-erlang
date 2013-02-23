-module(wordcount).

-export([main/1]).

-define(BUFSIZE, 1048576).
-define(FILEMODE, [raw, read, {read_ahead, ?BUFSIZE}]).

-define(SPACE, 32).
-define(LOWERCASE_DELTA, 32).

main(FileName) ->
    Freq = ets:new(freq, [public, {write_concurrency, true}]),
    Mgr = self(),
    spawn_link(fun () ->
                       %% one process is reading the file
                       {ok, Io} = file:open(FileName, ?FILEMODE),
                       fold_lines(Io, Mgr, fun map/4, fun reduce/2, Freq, 0)
               end),
    loop(Freq, 0, undefined).

%% main process keeping track of progress
loop(Freq, Done, Total) when Done =:= Total ->
    top(10, Freq);
loop(Freq, Done, Total) ->
    receive
        {total_lines, N} -> loop(Freq, Done    , N);
        {mapper_done, _} -> loop(Freq, Done + 1, Total);
        _                -> loop(Freq, Done    , Total)
    end.

fold_lines(Io, Mgr, Map, Reduce, Acc, Count) ->
    case file:read_line(Io) of
        {ok, Line} ->
            spawn_link(fun () -> Map(Line, Reduce, Acc, Mgr) end),
            fold_lines(Io, Mgr, Map, Reduce, Acc, Count+1);
        eof ->
            Mgr ! {total_lines, Count},
            Acc
    end.

%% each line is mapped and reduced in a separate process, all update
%% the shared state in ets
map(Line, Reduce, Freq, Mgr) ->
    Reduce(string:tokens(sanitize(Line), " "), Freq),
    Mgr ! {mapper_done, self()}.

reduce(Words, Freq) ->
    lists:foldl(fun (W, D) -> update_counter(W, D) end, Freq, Words).

top(N, Freq) ->
    L = [ {-C, {W, C}} || [{W, C}] <- ets:match(Freq, '$1') ],
    Sorted = orddict:from_list(L),
    do_top(N, Sorted).

do_top(0, _)  -> ok;
do_top(_, []) -> ok;
do_top(N, [{_, {W, C}}|T]) -> io:format("~s: ~w~n", [W, C]), do_top(N-1, T).

sanitize(Line) ->
    lists:map(fun (C) when C <  $A -> ?SPACE;
                  (C) when C =< $Z -> C + ?LOWERCASE_DELTA;
                  (C) when C <  $a -> ?SPACE;
                  (C) when C =< $z -> C;
                  (_)              -> ?SPACE
              end, Line).

update_counter(Word, Freq) ->
    ets:insert_new(Freq, {Word, 1}) orelse ets:update_counter(Freq, Word, 1),
    Freq.
