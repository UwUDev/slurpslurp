-- Optimization indexes for prepare_dataset.py performance
-- This script creates critical indexes for the bulk reply chain processing

-- Most critical index: referenced_message_id for building reply chains
-- This index is essential for the recursive CTE query performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_referenced_id
ON messages (referenced_message_id)
WHERE referenced_message_id IS NOT NULL;

-- Composite index for efficient filtering of valid messages
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_content_valid
ON messages (id)
WHERE content IS NOT NULL AND content != '';

-- Channel-based index for context lookups (if needed)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_channel_content
ON messages (channel_id, id)
WHERE content IS NOT NULL AND content != '';

-- Optimize query planning statistics
ANALYZE messages;

-- Optional: Create partial indexes for frequently queried channels
-- Uncomment and modify if you have specific high-traffic channels
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_hot_channels
-- ON messages (id, author_id, content)
-- WHERE channel_id IN (123456789, 987654321) -- Replace with actual channel IDs
-- AND content IS NOT NULL AND content != '';

-- Memory and performance settings for bulk operations
-- These settings optimize PostgreSQL for the 96GB RAM bulk processing workload
-- Note: These require superuser privileges to set

-- Uncomment if you have PostgreSQL admin access:
-- SET work_mem = '512MB';  -- Increase sort/hash memory
-- SET maintenance_work_mem = '2GB';  -- For index creation
-- SET shared_buffers = '24GB';  -- Use ~25% of 96GB RAM
-- SET effective_cache_size = '72GB';  -- ~75% of 96GB RAM
-- SET random_page_cost = '1.1';  -- Assuming SSD storage
-- SET cpu_tuple_cost = '0.01';
-- SET cpu_index_tuple_cost = '0.005';
-- SET cpu_operator_cost = '0.0025';

PRINT 'Database indexes optimized for prepare_dataset.py bulk processing.';
