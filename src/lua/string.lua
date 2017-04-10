local fun = require('fun')

-- Internal version without checks (assume, that input data is OK)
local function string_basesplit(inp, sep, max)
    local pos, last = 0, 1
    local done, match = false, inp:gmatch('(.-)' .. sep .. '()')
    return function()
        if done then return end
        pos = pos + 1
        if max then
            max = max - 1
        end
        local v, i = match()
        if inp == '' or sep == '' then
            done = true
            return pos, inp
        end
        if v == nil or (max and max < 0) then
            done = true
            return pos, inp:sub(last)
        end
        last = i
        return pos, v
    end
end

--- Split a string into a iterator of strings using delimiter.
-- this version is taken from http://lua-users.org/wiki/SplitJoin
-- @function gsplit
-- @string       inp  the string
-- @string[opt]  sep  a delimiter (defaults to whitespace)
-- @int[opt]     max  maximum number of splits
-- @returns           iterator
-- @usage for _, s in ("1 2 3"):split() do print(s) end
-- @usage fun.iter(("1 2 3"):split(' ', 1)):each(function(s) print(s) end)
local function string_gsplit(inp, sep, max)
    if type(inp) ~= 'string' then
        error("string.split argument #1 should be a string", 2)
    end
    if sep ~= nil and type(sep) ~= 'string' then
        error("string.split argument #2 should be a string or nil", 2)
    end
    if (max ~= nil and type(max) ~= 'number') or max < 0 then
        error("string.split argument #3 should be a number or nil", 2)
    end
    if (max < 0) then
        error("string.split argument #3 must be greater or equal 0", 2)
    end
    return string_basesplit(inp, sep, max)
end

--- Split a string into a list of strings using delimiter.
-- this version is taken from http://lua-users.org/wiki/SplitJoin
-- @function gsplit
-- @string       inp  the string
-- @string[opt]  sep  a delimiter (defaults to whitespace)
-- @int[opt]     max  maximum number of splits
-- @returns           table of strings
-- @usage #(("1 2 3"):split()) == 3
-- @usage #(("1 2 3"):split(' ', 1)) == 2
local function string_split(inp, sep, max)
    if type(inp) ~= 'string' then
        error("string.split argument #1 should be a string", 2)
    end
    if sep ~= nil and type(sep) ~= 'string' then
        error("string.split argument #2 should be a string or nil", 2)
    end
    if (max ~= nil and type(max) ~= 'number') or max < 0 then
        error("string.split argument #3 should be a number or nil", 2)
    end
    if (max < 0) then
        error("string.split argument #3 must be greater or equal 0", 2)
    end
    return fun.iter(string_basesplit(inp, sep, max)):totable()
end

-- It'll automatically set string methods, too.
string.gsplit = string_gsplit
string.split  = string_split
