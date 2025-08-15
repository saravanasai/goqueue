local processing_queue = KEYS[1]
local job_payload = ARGV[1]

-- Remove job from processing queue if it exists
local removed = redis.call('LREM', processing_queue, 1, job_payload)
return removed
