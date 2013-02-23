-module(wordcount).

-export([main/1]).

-define(BUFSIZE, 1048576).
-define(FILEMODE, [raw, read, {read_ahead, ?BUFSIZE}]).

-define(SPACE, 32).
-define(LOWERCASE_DELTA, 32).

main(FileName) ->
    {ok, Io} = file:open(FileName, ?FILEMODE),
    Freq = fold_lines(Io, fun map/1, fun reduce/2, ets:new(freq, [])),
    top(10, Freq).

fold_lines(Io, Map, Reduce, Acc) ->
    case file:read_line(Io) of
        {ok, Line} -> fold_lines(Io, Map, Reduce, Reduce(Map(Line), Acc));
        eof        -> Acc
    end.

map(Line) ->
    string:tokens(sanitize(Line), " ").

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
