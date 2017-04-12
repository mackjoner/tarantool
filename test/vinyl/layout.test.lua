test_run = require('test_run').new()
test_run:cmd('restart server default with cleanup=1')

fiber = require 'fiber'
fio = require 'fio'
xlog = require 'xlog'
fun = require 'fun'

space = box.schema.space.create('test', {engine='vinyl'})
_ = space:create_index('pk')

space:replace({1})
space:replace({2})
space:replace({3})
box.snapshot()

space:replace({4})
space:replace({5})
space:replace({6})
box.snapshot()
space:drop()

-- Get the list of files from the last checkpoint.
-- convert names to relative
work_dir = require('fio').cwd()
files = box.backup.start()
for i, name in pairs(files) do files[i] = name:sub(#work_dir + 2) end
table.sort(files)
files
result = {}
test_run:cmd("setopt delimiter ';'")
for i, path in pairs(files) do
    local suffix = string.gsub(path, '.*%.', '')
    if suffix ~= 'snap' and suffix ~= 'xlog' then
        local rows = {}
        local i = 1
        for lsn, row in xlog.pairs(path) do
            rows[i] = row
            i = i + 1
        end
        table.insert(result, { path, rows })
    end
end;

test_run:cmd("setopt delimiter ''");

box.backup.stop() -- resume the garbage collection process

test_run:cmd("push filter 'timestamp: .*' to 'timestamp: <timestamp>'")
result
test_run:cmd("clear filter")
