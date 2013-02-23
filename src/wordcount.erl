-module(wordcount).

-export([main/1]).

-define(BUFSIZE, 1048576).
-define(FILEMODE, [raw, binary, read, {read_ahead, ?BUFSIZE}]).

-define(SPACE, 32).
-define(LOWERCASE_DELTA, 32).

main(FileName) ->
    {ok, Io} = file:open(FileName, ?FILEMODE),
    Freq = fold_lines(Io, fun map/1, fun reduce/2, dict:new()),
    top(10, Freq).

fold_lines(Io, Map, Reduce, Acc) ->
    case file:read_line(Io) of
        {ok, Line} -> fold_lines(Io, Map, Reduce, Reduce(Map(Line), Acc));
        eof        -> Acc
    end.

map(Line) ->
    [ W || W <- binary:split(sanitize(Line), <<$ >>, [global]), W =/= <<>> ].

reduce(Words, Freq) ->
    lists:foldl(fun (W, D) -> dict:update_counter(W, 1, D) end, Freq, Words).

top(N, Freq) ->
    L = [ {-C, {W, C}} || {W, C} <- dict:to_list(Freq) ],
    Sorted = orddict:from_list(L),
    do_top(N, Sorted).

do_top(0, _)  -> ok;
do_top(_, []) -> ok;
do_top(N, [{_, {W, C}}|T]) -> io:format("~s: ~w~n", [W, C]), do_top(N-1, T).

sanitize(Line) ->
    << <<(sanitize_char(C)):8>> || <<C>> <= Line >>.

sanitize_char(C) when C <  $A -> ?SPACE;
sanitize_char(C) when C =< $Z -> C + ?LOWERCASE_DELTA;
sanitize_char(C) when C <  $a -> ?SPACE;
sanitize_char(C) when C =< $z -> C;
sanitize_char(_)              -> ?SPACE.
