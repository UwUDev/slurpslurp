-- id is the ID of the attachment you want to look up at the beginning of the filename before the '_'

SELECT * FROM messages
WHERE EXISTS (
    SELECT 1 FROM jsonb_array_elements(attachments) elem
    WHERE elem->>'id' = '...'
);