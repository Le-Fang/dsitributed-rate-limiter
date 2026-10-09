-- Protocol v1: exactly one hash and eight immutable arguments.
local MAX = 9007199254740991
local LIMIT = 1000000000000
local RETENTION = 86400000
local function integer(n) return string.format('%.0f', n) end
local function failure(code) return {code, '-1', '-1', '-1', '-1', '-1', '-1', '0', '1'} end
local function number(s, low, high)
    if type(s) ~= 'string' or #s > 17 then return nil end
    if s ~= '0' and not string.match(s, '^[1-9][0-9]*$') and not string.match(s, '^%-[1-9][0-9]*$') then return nil end
    local n = tonumber(s)
    if not n or n < low or n > high or integer(n) ~= s then return nil end
    return n
end
local function identity(s) return type(s) == 'string' and #s == 32 and string.match(s, '^[0-9a-f]+$') end
-- Parse the deliberately small JSON string-only object without accepting duplicate
-- fields (cjson alone silently collapses them). Reject before any mutation.
local function session(raw)
    if type(raw) ~= 'string' or #raw > 512 then return nil end
    local object, pos = {}, 1
    local function skip() local _, e = string.find(raw, '^%s*', pos); pos = (e or pos - 1) + 1 end
    skip()
    if string.sub(raw, pos, pos) ~= '{' then return nil end
    pos = pos + 1
    for i = 1, 5 do
        skip()
        local a, b, key, value = string.find(raw, '^"([a-z_]+)"%s*:%s*"([0-9a-f]+)"', pos)
        if not a or object[key] then return nil end
        object[key], pos = value, b + 1
        skip()
        if string.sub(raw, pos, pos) ~= (i == 5 and '}' or ',') then return nil end
        pos = pos + 1
    end
    skip()
    if pos ~= #raw + 1 or not identity(object.id) then return nil end
    local seq = number(object.seq, 0, MAX)
    local delta = number(object.delta, 0, 1000000)
    local cap = number(object.cap, 1, 1000000)
    local expires = number(object.expires_ms, 0, MAX)
    if not seq or not delta or not cap or not expires or delta > cap or (seq == 0) ~= (delta == 0) then return nil end
    return {id=object.id, seq=seq, delta=delta, cap=cap, expires=expires}
end
if #KEYS ~= 1 or #ARGV ~= 8 then return failure('INVALID_ARGUMENT') end
local op, id = ARGV[1], ARGV[2]
local slot = number(ARGV[3], -1, 255)
local seq = number(ARGV[4], 0, MAX)
local delta = number(ARGV[5], 0, 1000000)
local rate = number(ARGV[6], 1, LIMIT)
local burst = number(ARGV[7], 1000000, LIMIT)
local deadline = number(ARGV[8], 0, MAX)
if not identity(id) or not slot or not seq or not delta or not rate or not burst or burst % 1000000 ~= 0 or not deadline then return failure('INVALID_ARGUMENT') end
if op == 'INIT' then
    if slot ~= -1 or seq ~= 0 or delta == 0 or deadline == 0 then return failure('INVALID_ARGUMENT') end
elseif op == 'REFRESH' then
    if slot < 0 or seq ~= 0 or delta ~= 0 or deadline ~= 0 then return failure('INVALID_ARGUMENT') end
elseif op == 'APPLY' then
    if slot < 0 or seq == 0 or delta == 0 or deadline ~= 0 then return failure('INVALID_ARGUMENT') end
else return failure('INVALID_ARGUMENT') end
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if now < 0 or now > MAX then return failure('NUMERIC_LIMIT') end
local kind = redis.call('TYPE', KEYS[1]).ok
if kind ~= 'none' and kind ~= 'hash' then return failure('STATE_CORRUPT') end
if kind == 'none' and op ~= 'INIT' then return failure('SESSION_EXPIRED') end
local balance, last, rem = burst, now, 0
if kind == 'hash' then
    local g = redis.call('HMGET', KEYS[1], 'v', 'rate_u', 'burst_u', 'balance_u', 'last_ms', 'rem')
    if not g[1] then return failure('STATE_CORRUPT') end
    if g[1] ~= '1' then return failure('VERSION_MISMATCH') end
    local old_rate, old_burst = number(g[2], 1, LIMIT), number(g[3], 1000000, LIMIT)
    balance, last, rem = number(g[4], -LIMIT, LIMIT), number(g[5], 0, MAX), number(g[6], 0, 999)
    if not old_rate or not old_burst or old_burst % 1000000 ~= 0 or not balance or not last or not rem or balance > old_burst or (balance == old_burst and rem ~= 0) then return failure('STATE_CORRUPT') end
    if old_rate ~= rate or old_burst ~= burst then return failure('POLICY_MISMATCH') end
end
local logical = math.max(now, last)
if logical > MAX - RETENTION then return failure('NUMERIC_LIMIT') end
local chosen, fresh = nil, '0'
if op == 'INIT' then
    if logical >= deadline then return failure('INIT_EXPIRED') end
    if deadline - logical > 30000 then return failure('INVALID_ARGUMENT') end
    local free, matched = nil, nil
    for i = 0, 255 do
        local raw = redis.call('HGET', KEYS[1], 's:' .. integer(i))
        if raw then
            local s = session(raw)
            if not s then return failure('STATE_CORRUPT') end
            if s.id == id then
                if matched then return failure('STATE_CORRUPT') end
                matched, chosen = i, s
            end
            if s.expires <= logical and not free then free = i end
        elseif not free then free = i end
    end
    if matched then
        slot = matched
        if chosen.expires <= logical then return failure('SESSION_EXPIRED') end
        if chosen.cap ~= delta then return failure('INVALID_ARGUMENT') end
    else
        if not free then return failure('SESSION_CAPACITY') end
        slot, fresh = free, '1'
        chosen = {id=id, seq=0, delta=0, cap=delta}
    end
else
    local raw = redis.call('HGET', KEYS[1], 's:' .. integer(slot))
    if not raw then return failure('SESSION_EXPIRED') end
    chosen = session(raw)
    if not chosen then return failure('STATE_CORRUPT') end
    if chosen.id ~= id or chosen.expires <= logical then return failure('SESSION_EXPIRED') end
    if op == 'APPLY' and delta > chosen.cap then return failure('INVALID_ARGUMENT') end
end
-- deficit*1000 is at most 2e15, within the exact integer range.
local elapsed, deficit = logical - last, burst - balance
local fill_ms = math.ceil((deficit * 1000 - rem) / rate)
if elapsed >= fill_ms then
    balance, rem = burst, 0
else
    local fractional = elapsed * (rate % 1000) + rem
    balance = balance + elapsed * math.floor(rate / 1000) + math.floor(fractional / 1000)
    rem = fractional % 1000
end
local code = 'OK'
if op == 'APPLY' then
    if seq == chosen.seq and delta == chosen.delta then code = 'DUPLICATE'
    elseif chosen.seq < MAX and seq == chosen.seq + 1 then
        balance = balance - delta * 1000000
        if balance < -LIMIT then return failure('NUMERIC_LIMIT') end
        chosen.seq, chosen.delta = seq, delta
    else return failure('SEQUENCE_ERROR') end
end
local retry = math.max(0, math.ceil(((1000000 - balance) * 1000 - rem) / rate))
local gap = logical - now
local ttl = math.max(RETENTION, math.ceil(((burst - balance) * 1000 - rem) / rate) + 60000)
-- Bound relative and absolute expiry before PERSIST; all additions remain exact.
if ttl > MAX - logical then return failure('NUMERIC_LIMIT') end
ttl = ttl + gap
local expiry = logical + RETENTION
local session_json = cjson.encode({id=id, seq=integer(chosen.seq), delta=integer(chosen.delta), cap=integer(chosen.cap), expires_ms=integer(expiry)})
local rate_s, burst_s = integer(rate), integer(burst)
local balance_s, logical_s, rem_s = integer(balance), integer(logical), integer(rem)
local session_field, ttl_s = 's:' .. integer(slot), integer(ttl)
local response = {code, integer(slot), integer(chosen.seq), balance_s, integer(retry), integer(RETENTION + gap), logical_s, fresh, '1'}
-- COMMIT: all validation, arithmetic, encoding and response construction precede writes.
redis.call('PERSIST', KEYS[1])
redis.call('HSET', KEYS[1], 'v', '1', 'rate_u', rate_s, 'burst_u', burst_s,
    'balance_u', balance_s, 'last_ms', logical_s, 'rem', rem_s, session_field, session_json)
redis.call('PEXPIRE', KEYS[1], ttl_s)
return response
