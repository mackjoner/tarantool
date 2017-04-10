#!/usr/bin/env tarantool

local tap = require('tap')
local test = tap.test("string extensions")
test:plan(16)

test:is_deeply((""):split(""), {""},   "empty split")
test:is_deeply((""):split("z"), {""},  "empty split")
test:is_deeply(("a"):split(""), {"a"}, "empty split")
test:is_deeply(("a"):split("a"), {"", ""}, "split self")
test:is_deeply(
    (" 1 2  3  "):split(),
    {"1", "2", "3"},
    "complex split on empty separator"
)
test:is_deeply(
    (" 1 2  3  "):split(" "),
    {"", "1", "2", "", "3", "", ""},
    "complex split on space separator"
)
test:is_deeply(
    (" 1 2  \n\n\n\r\t\n3  "):split("%s+"),
    {"", "1", "2", "3", ""},
    "complex split on non-empty regexp separator"
) -- also used for regexp splitting testing
test:is_deeply(
    ("a*bb*c*ddd"):split("*"),
    {"a", "bb", "c", "ddd"},
    "another * separator"
)
test:is_deeply(
    ("dog:fred:bonzo:alice"):split(":", 3),
    {"dog", "fred", "bonzo:alice"},
    "testing max separator"
)
test:is_deeply(
    ("///"):split("/"),
    {"", "", "", ""},
    "testing splitting on one char"
)

-- Testing iterator version
local result = {"dog", "fred", "bonzo:alice"}
for pos, val in ("dog:fred:bonzo:alice"):gsplit(":", 3) do
    test:is(val, result[pos], "checking position " .. pos .. " using for")
end
("dog:fred:bonzo:alice"):gsplit(":", 3):enumerate():each(function(pos, val)
    test:is(val, result[pos], "checking position " .. pos .. " using fun")
end)
