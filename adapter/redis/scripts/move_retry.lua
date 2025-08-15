local retry_key = KEYS[1]
local main_queue = KEYS[2]
local current_time = tonumber(ARGV[1])

-- Get jobs that should be retried (score <= current_time)
local jobs = redis.call('ZRANGEBYSCORE', retry_key, '-inf', current_time, 'LIMIT', 0, 10)

if #jobs == 0 then
    return 0
end

-- Move each job from retry queue to main queue
local moved = 0
for _, job in ipairs(jobs) do
    -- Remove from retry queue and push to main queue atomically
    local removed = redis.call('ZREM', retry_key, job)
    if removed == 1 then
        redis.call('LPUSH', main_queue, job)
        moved = moved + 1
    end
end

return moved
