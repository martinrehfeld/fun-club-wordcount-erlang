-module(wordcount).

-export([main/1]).

-define(BUFSIZE, 1048576).
-define(FILEMODE, [raw, read, {read_ahead, ?BUFSIZE}]).


main(InputFileName) ->
    {ok, Device} = file:open(InputFileName, ?FILEMODE),
    each_line(Device, fun noop/2, nostate).

each_line(Device, Fun, Acc) ->
    case file:read_line(Device) of
        {ok, Line} ->
            NewAcc = Fun(Line, Acc),
            each_line(Device, Fun, NewAcc);

        eof  ->
            Acc
    end.

noop(_Line, Acc) ->
    Acc.
