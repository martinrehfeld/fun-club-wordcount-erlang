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
                       Process =
                           fun (Line) ->
                                   %% each line is mapped and reduced in a
                                   %% separate process, all update the shared
                                   %% state in ets
                                   spawn_link(fun () ->
                                           process(Line, Freq),
                                           Mgr ! mapper_done
                                   end)
                           end,
                       LineCount = foreach_lines(Io, Process),
                       Mgr ! {total_lines, LineCount}
               end),
    loop(Freq, 0, undefined).

%% main process keeping track of progress
loop(Freq, Done, Total) when Done =:= Total ->
    top(10, Freq);
loop(Freq, Done, Total) ->
    receive
        {total_lines, N} -> loop(Freq, Done    , N);
        mapper_done      -> loop(Freq, Done + 1, Total);
        _                -> loop(Freq, Done    , Total)
    end.

foreach_lines(Io, Fun) ->
    foreach_lines(Io, Fun, 0).

foreach_lines(Io, Process, Count) ->
    case file:read_line(Io) of
        {ok, Line} ->
            Process(Line),
            foreach_lines(Io, Process, Count+1);
        eof ->
            Count
    end.

process(Line, Freq) ->
    Words = string:tokens(sanitize(Line), " "),
    lists:foreach(fun (W) -> update_counter(W, Freq) end, Words).

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
