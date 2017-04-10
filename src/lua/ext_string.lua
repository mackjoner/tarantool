local fun = require('fun')

-- Internal version without checks (assume, that input data is OK)
local function string_basesplit_iter(inp, sep, max)
    local last    = 1
    local pos     = 0
    local max     = max or 100000000000000
    local done    = false
    local matcher = inp:gmatch('(.-)' .. sep .. '()')
    return fun.wrap(function()
        pos, max = pos + 1, max - 1
        if done then
            return
        end
        local v, i = matcher()
        if inp == '' or sep == '' then
            done = true
            return pos, inp
        end
        if v == nil or (max and max == 0) then
            done = true
            return pos, inp:sub(last)
        end
        last = i
        return pos, v
    end)
end

local function string_basesplit_iter_emptysep(inp, max)
    return string_basesplit_iter(inp, "%s+", max):filter(function(inp)
        return #inp > 0
    end)
end

local function string_basesplit(inp, sep, max)
    if sep == nil then
        return string_basesplit_iter_emptysep(inp, max)
    end
    return string_basesplit_iter(inp, sep, max)
end

--- Split a string into a iterator of strings using delimiter.
-- this version is taken from http://lua-users.org/wiki/SplitJoin
-- @function gsplit
-- @string       inp  the string
-- @string[opt]  sep  a delimiter (defaults to whitespace)
-- @int[opt]     max  maximum number of splits (>= 0)
-- @returns           iterator
-- @usage for _, s in ("1 2 3"):split() do print(s) end
-- @usage fun.iter(("1 2 3"):split(' ', 1)):each(function(s) print(s) end)
local function string_gsplit(inp, sep, max)
    if type(inp) ~= 'string' then
        error("string.gsplit argument #1 should be a string", 2)
    end
    if sep ~= nil and type(sep) ~= 'string' then
        error("string.gsplit argument #2 should be a string or nil", 2)
    end
    if max ~= nil and type(max) ~= 'number'  then
        error("string.gsplit argument #3 should be a number or nil", 2)
    end
    if max and max < 0 then
        error("string.gsplit argument #3 must be greater or equal 0", 2)
    end
    return string_basesplit(inp, sep, max)
end

--- Split a string into a list of strings using delimiter.
-- this version is taken from http://lua-users.org/wiki/SplitJoin
-- @function gsplit
-- @string       inp  the string
-- @string[opt]  sep  a delimiter (defaults to whitespace)
-- @int[opt]     max  maximum number of splits (>= 0)
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
    if max ~= nil and type(max) ~= 'number' then
        error("string.split argument #3 should be a number or nil", 2)
    end
    if max and max < 0 then
        error("string.split argument #3 must be greater or equal 0", 2)
    end
    return string_basesplit(inp, sep, max):totable()
end

--- Left-justify string in a field of given width.
-- Append "width - len(inp)" chars to given string. Input is never trucated.
-- @function ljust
-- @string       inp    the string
-- @int          width  at least bytes to be returned
-- @string[opt]  char   char of length 1 to fill with (" " by default)
-- @returns             result string
local function string_ljust(inp, width, char)
    if type(inp) ~= 'string' then
        error("string.ljust argument #1 should be a string", 2)
    end
    if type(width) ~= 'number' or width < 0 then
        error("string.ljust argument #2 should be a natural number", 2)
    end
    if char ~= nil and (type(char) ~= 'string' or #char ~= 1) then
        error("string.ljust argument #3 should be a char (length 1) or nil", 2)
    end
    char = char or " "
    local delta = width - #inp
    if delta < 0 then
        return inp
    end
    return inp .. char:rep(delta)
end

--- Right-justify string in a field of given width.
-- Prepend "width - len(inp)" chars to given string. Input is never trucated.
-- @function rjust
-- @string       inp    the string
-- @int          width  at least bytes to be returned
-- @string[opt]  char   char of length 1 to fill with (" " by default)
-- @returns             result string
local function string_rjust(inp, width, char)
    if type(inp) ~= 'string' then
        error("string.rjust argument #1 should be a string", 2)
    end
    if type(width) ~= 'number' or width < 0 then
        error("string.rjust argument #2 should be a natural number", 2)
    end
    if char ~= nil and (type(char) ~= 'string' or #char ~= 1) then
        error("string.rjust argument #3 should be a char (length 1) or nil", 2)
    end
    char = char or " "
    local delta = width - #inp
    if delta < 0 then
        return inp
    end
    return char:rep(delta) .. inp
end

--- Center string in a field of given width.
-- Prepend and append "(width - len(inp))/2" chars to given string.
-- Input is never trucated.
-- @function rjust
-- @string       inp    the string
-- @int          width  at least bytes to be returned
-- @string[opt]  char   char of length 1 to fill with (" " by default)
-- @returns             result string
local function string_center(inp, width, char)
    if type(inp) ~= 'string' then
        error("string.center argument #1 should be a string", 2)
    end
    if type(width) ~= 'number' or width < 0 then
        error("string.center argument #2 should be a natural number", 2)
    end
    if char ~= nil and (type(char) ~= 'string' or #char ~= 1) then
        error("string.center argument #3 should be a char (length 1) or nil", 2)
    end
    char = char or " "
    local delta = width - #inp
    if delta < 0 then
        return inp
    end
    local pad_left = math.floor(delta / 2)
    local pad_right = delta - pad_left
    return char:rep(pad_left) .. inp .. char:rep(pad_right)
end

-- It'll automatically set string methods, too.
string.gsplit = string_gsplit
string.split  = string_split
string.ljust  = string_ljust
string.rjust  = string_rjust
string.center = string_center
