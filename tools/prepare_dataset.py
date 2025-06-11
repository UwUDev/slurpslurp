import psycopg2
import psycopg2.extras
import json
import re
import argparse
import sys
import random
from tqdm import tqdm
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict
import gc

MAX_INPUT_CHARS = float('inf')
MAX_OUTPUT_CHARS = float('inf')
MIN_VALIDATION_EXAMPLES = 10
MAX_VALIDATION_EXAMPLES = 5000
MIN_PAIRS_FOR_SPLIT = 20
MAX_CONTEXT_MESSAGES = float('inf')
CONTEXT_TIME_LIMIT_MINUTES = float('inf')

# Performance optimization constants
BATCH_SIZE = 2000  # Increased batch size for better throughput
MAX_WORKERS = 16   # Utilize more cores on 96GB system
MEMORY_EFFICIENT = True    parser.add_argument(
        "--max-workers", type=int, default=16,
        help="Number of parallel workers for processing (default: 16, optimized for 96GB RAM)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=2000,
        help="Batch size for parallel processing (default: 2000, optimized for throughput)"
    )
    parser.add_argument(
        "--turbo-mode", action="store_true",
        help="Enable maximum performance mode (uses more CPU and memory)"
    )memory-efficient processing
CHUNK_SIZE = 10000  # Process data in larger chunks

DISCORD_EPOCH = 1420070400000

def discord_id_to_timestamp(discord_id: int) -> datetime:
    """
    Convertit un ID Discord en timestamp datetime.
    Les IDs Discord contiennent un timestamp Unix avec l'époque Discord (2015-01-01).
    """
    timestamp_ms = (discord_id >> 22) + DISCORD_EPOCH
    return datetime.utcfromtimestamp(timestamp_ms / 1000)

USER_PART_TEMPLATE = """CONTEXT:
{context_messages}
---
CURRENT: {current_message}"""

MODEL_PART_TEMPLATE = """{response_message}"""

def format_user_content(context_messages: str, current_message: str, template: str | None = None) -> str:
    """Formate le contenu user avec le template."""
    if template is None:
        template = USER_PART_TEMPLATE
    assert template is not None
    return template.format(
        context_messages=context_messages,
        current_message=current_message
    ).strip()

def format_model_content(response_message: str, template: str | None = None) -> str:
    """Formate le contenu model avec le template."""
    if template is None:
        template = MODEL_PART_TEMPLATE
    assert template is not None
    return template.format(
        response_message=response_message
    ).strip()

def preprocess_text(text: str, allowed_users: set | None = None) -> str:
    if not isinstance(text, str):
        return ""
    if allowed_users:
        text = re.sub(
            r"<@!?(\d+)>",
            lambda m: m.group(0) if m.group(1) in allowed_users else "@user",
            text,
        )
    else:
        text = re.sub(r"<@!?\d+>", "@user", text)

    text = re.sub(r"<@&\d+>", "@role", text)
    text = re.sub(r"<#\d+>", "#channel", text)

    if re.search(r"https?://\S+", text):
        return ""

    if re.search(r"``````", text, flags=re.DOTALL):
        return ""

    text = re.sub(r":[a-zA-Z0-9-_]{3,32}:", "", text)

    text = re.sub(r"(\*\*|__|\*|_|~~)(.*?)\1", r"\2", text)
    text = re.sub(r"<a?:(\w+):\d+>", r":\1:", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def is_meaningful_exchange(input_text: str, output_text: str) -> bool:
    if len(output_text.split()) < 3:
        return False

    reaction_patterns = [r'^[!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]+$', r"^[0-9]+$"]

    if any(
        re.match(pattern, output_text.lower().strip()) for pattern in reaction_patterns
    ):
        return False

    if input_text.lower().strip() == output_text.lower().strip():
        return False

    return True


def get_conversation_context_optimized(
    messages_by_channel: dict,
    message_id: int,
    channel_id: int,
    message_timestamp,
    max_context: int | float = MAX_CONTEXT_MESSAGES,
) -> list:
    """
    Optimized version that works with pre-loaded in-memory data.
    """
    if channel_id not in messages_by_channel:
        return []

    channel_messages = messages_by_channel[channel_id]

    # Find messages before the current message in the same channel
    context_messages = []
    for msg_id, content, author_id, timestamp in channel_messages:
        if msg_id >= message_id:
            continue

        if CONTEXT_TIME_LIMIT_MINUTES != float('inf'):
            time_limit = message_timestamp - timedelta(minutes=CONTEXT_TIME_LIMIT_MINUTES)
            if timestamp < time_limit:
                continue

        processed_content = preprocess_text(content)
        if processed_content and len(processed_content.split()) >= 2:
            context_messages.append((content, author_id, timestamp))

    # Sort by timestamp and limit
    context_messages.sort(key=lambda x: x[2])

    if max_context != float('inf'):
        context_messages = context_messages[-max_context:]

    return context_messages


def get_conversation_participants(
    context_messages: list, input_author_id: str, output_author_id: str
) -> set:
    participants = {str(input_author_id), str(output_author_id)}

    for _, author_id, _ in context_messages:
        participants.add(str(author_id))

    return participants


def preload_channel_messages(db_dsn: str, channel_ids: set) -> dict:
    """
    Preload all messages from specific channels for context lookup.
    This eliminates the need for repeated database queries during processing.
    """
    if not channel_ids:
        return {}

    print(f"[*] Preloading context messages from {len(channel_ids)} channels...")

    messages_by_channel = defaultdict(list)

    try:
        with psycopg2.connect(db_dsn) as conn:
            with conn.cursor() as cursor:
                # Use ANY() for efficient bulk query instead of IN clause
                cursor.execute("""
                    SELECT id, channel_id, content, author_id
                    FROM messages
                    WHERE channel_id = ANY(%s)
                    AND content IS NOT NULL
                    AND content != ''
                    ORDER BY channel_id, id
                """, (list(channel_ids),))

                for row in cursor:
                    msg_id, channel_id, content, author_id = row
                    timestamp = discord_id_to_timestamp(msg_id)
                    messages_by_channel[channel_id].append((msg_id, content, author_id, timestamp))

    except psycopg2.Error as e:
        print(f"[WARNING] Could not preload channel messages: {e}")
        return {}

    print(f"[+] Preloaded {sum(len(msgs) for msgs in messages_by_channel.values())} context messages.")
    return dict(messages_by_channel)


def get_message_chains_from_db(db_dsn: str) -> list:
    """
    Optimized version: Bulk loads all data into memory, then processes chains in-memory.
    This dramatically reduces database query time from hours to minutes.
    """
    print(f"[*] Connecting to PostgreSQL database...")

    try:
        # Use connection with optimized settings
        with psycopg2.connect(db_dsn) as conn:
            # Optimize connection for bulk operations
            with conn.cursor() as setup_cursor:
                try:
                    setup_cursor.execute("SET work_mem = '512MB';")
                    setup_cursor.execute("SET maintenance_work_mem = '2GB';")
                    setup_cursor.execute("SET temp_buffers = '256MB';")
                    setup_cursor.execute("SET synchronous_commit = OFF;")
                    conn.commit()
                    print("[+] Database session optimized for bulk operations")
                except psycopg2.Error:
                    pass  # Continue if we can't set these

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:

                # First, ensure we have the critical index for performance
                print("[*] Ensuring database indexes are optimized...")
                try:
                    cursor.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_referenced_id ON messages (referenced_message_id) WHERE referenced_message_id IS NOT NULL;")
                    cursor.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_channel_id ON messages (channel_id);")
                    cursor.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_content ON messages (id) WHERE content IS NOT NULL AND content != '';")
                    conn.commit()
                except psycopg2.Error as e:
                    print(f"[WARNING] Could not create indexes (may already exist): {e}")
                    conn.rollback()

                # Step 1: Bulk load all relevant messages into memory
                print("[*] Bulk loading all messages with replies and their replies...")

                # Optimized query with performance hints
                bulk_query = """
                WITH RECURSIVE reply_chains AS (
                    -- Base case: all root messages that have replies
                    SELECT DISTINCT m.id, m.channel_id, m.content, m.author_id, 0 as depth, m.id as root_id
                    FROM messages m
                    WHERE EXISTS (
                        SELECT 1 FROM messages r
                        WHERE r.referenced_message_id = m.id
                        AND r.content IS NOT NULL
                        AND r.content != ''
                        LIMIT 1  -- Early termination optimization
                    )
                    AND m.content IS NOT NULL
                    AND m.content != ''
                    AND char_length(m.content) >= 3  -- Filter very short messages early

                    UNION ALL

                    -- Recursive case: all replies in the chain
                    SELECT r.id, r.channel_id, r.content, r.author_id, rc.depth + 1, rc.root_id
                    FROM messages r
                    INNER JOIN reply_chains rc ON r.referenced_message_id = rc.id
                    WHERE r.content IS NOT NULL
                    AND r.content != ''
                    AND char_length(r.content) >= 3
                    AND rc.depth < 20  -- Reduced depth for faster processing
                )
                SELECT id, channel_id, content, author_id, depth, root_id
                FROM reply_chains
                ORDER BY root_id, depth, id;
                """

                cursor.execute(bulk_query)
                all_messages = cursor.fetchall()

                print(f"[+] Loaded {len(all_messages)} messages from database into memory.")

                # Step 2: Group messages by root_id to form chains
                print("[*] Building reply chains in memory...")
                chains_dict = defaultdict(list)

                for msg in all_messages:
                    timestamp = discord_id_to_timestamp(msg['id'])
                    msg_tuple = (msg['id'], msg['channel_id'], msg['content'], msg['author_id'], timestamp)
                    chains_dict[msg['root_id']].append(msg_tuple)

                # Step 3: Sort each chain chronologically and filter valid chains
                all_chains = []
                for root_id, chain in chains_dict.items():
                    # Sort by timestamp (already sorted by id in SQL, but just to be sure)
                    chain.sort(key=lambda x: x[0])  # Sort by message ID (chronological)

                    if len(chain) >= 2:  # Only keep chains with at least 2 messages
                        all_chains.append(chain)

                print(f"[+] {len(all_chains)} complete reply chains built in memory.")

                # Clear memory
                del all_messages
                del chains_dict
                gc.collect()

                return all_chains

    except psycopg2.Error as e:
        print(f"[ERROR] PostgreSQL database error : {e}", file=sys.stderr)
        sys.exit(1)



def create_chain_record(chain: list, target_bot_id: str | None = None, user_template: str | None = None, model_template: str | None = None) -> tuple:
    """
    Crée un enregistrement d'entraînement à partir d'une chaîne de messages.
    La chaîne est formatée comme une conversation multi-tour utilisant les templates.
    """
    if len(chain) < 2:
        return None, 0

    participants = set()
    for msg in chain:
        participants.add(str(msg[3]))

    conversation_parts = []
    total_chars = 0

    for i in range(len(chain) - 1):
        current_msg = chain[i]
        next_msg = chain[i + 1]

        msg_id, channel_id, content, author_id, timestamp = current_msg
        next_msg_id, _, next_content, next_author_id, _ = next_msg

        processed_content = preprocess_text(content, participants)
        processed_next_content = preprocess_text(next_content, participants)

        if not processed_content or not processed_next_content:
            continue

        context_messages = []
        for j in range(0, i):
            ctx_msg = chain[j]
            ctx_content = preprocess_text(ctx_msg[2], participants)
            if ctx_content:
                context_messages.append(f"<@{ctx_msg[3]}>: {ctx_content}")

        context_str = "\n".join(context_messages) if context_messages else "[Aucun contexte]"
        current_formatted = f"<@{author_id}>: {processed_content}"
        next_formatted = f"<@{next_author_id}>: {processed_next_content}"

        user_role = "user"
        model_role = "model"
        if target_bot_id:
            if str(next_author_id) == str(target_bot_id):
                model_role = "model"
            else:
                model_role = "user"
                user_role = "user"

        user_content = format_user_content(context_str, current_formatted, user_template)
        model_content = format_model_content(next_formatted, model_template)

        total_new_chars = len(user_content) + len(model_content)
        if MAX_INPUT_CHARS != float('inf') and total_chars + total_new_chars > MAX_INPUT_CHARS:
            break

        conversation_parts.append({
            "role": user_role,
            "parts": [{"text": user_content}]
        })
        conversation_parts.append({
            "role": model_role,
            "parts": [{"text": model_content}]
        })

        total_chars += total_new_chars

    if len(conversation_parts) < 2:
        return None, 0

    return {"contents": conversation_parts}, total_chars


def process_chain_batch_optimized(chains_batch, target_bot_id, user_template, model_template):
    """
    Optimized batch processing with faster text processing and early filtering.
    """
    batch_records = []

    # Pre-compile regex patterns for better performance
    mention_pattern = re.compile(r"<@!?\d+>")
    role_pattern = re.compile(r"<@&\d+>")
    channel_pattern = re.compile(r"<#\d+>")
    url_pattern = re.compile(r"https?://\S+")

    for chain in chains_batch:
        if len(chain) < 2:
            continue

        # Quick validation before expensive processing
        valid_chain = True
        for msg in chain:
            content = msg[2]  # content is at index 2
            if not content or len(content.strip()) < 3:
                valid_chain = False
                break
            # Quick URL check - skip chains with URLs
            if url_pattern.search(content):
                valid_chain = False
                break

        if not valid_chain:
            continue

        try:
            json_record, total_length = create_chain_record_optimized(
                chain, target_bot_id, user_template, model_template,
                mention_pattern, role_pattern, channel_pattern, url_pattern
            )

            if json_record and json_record.get("contents"):
                contents = json_record["contents"]
                if len(contents) >= 2:
                    # Quick meaningful exchange check
                    last_text = contents[-1]["parts"][0]["text"]
                    first_text = contents[0]["parts"][0]["text"]

                    # Simple word count check instead of complex regex
                    if len(last_text.split()) >= 3 and len(first_text.split()) >= 3:
                        batch_records.append(json_record)

        except Exception:
            # Silent continue for performance
            continue

    return batch_records


def create_chain_record_optimized(chain, target_bot_id, user_template, model_template,
                                 mention_pattern, role_pattern, channel_pattern, url_pattern):
    """
    Optimized chain record creation with pre-compiled patterns.
    """
    if len(chain) < 2:
        return None, 0

    participants = set(str(msg[3]) for msg in chain)  # Extract all author_ids
    conversation_parts = []
    total_chars = 0

    for i in range(len(chain) - 1):
        current_msg = chain[i]
        next_msg = chain[i + 1]

        msg_id, channel_id, content, author_id, timestamp = current_msg
        next_msg_id, _, next_content, next_author_id, _ = next_msg

        # Fast preprocessing with pre-compiled patterns
        processed_content = preprocess_text_optimized(
            content, participants, mention_pattern, role_pattern, channel_pattern, url_pattern
        )
        processed_next_content = preprocess_text_optimized(
            next_content, participants, mention_pattern, role_pattern, channel_pattern, url_pattern
        )

        if not processed_content or not processed_next_content:
            continue

        # Simplified context building
        context_messages = []
        for j in range(max(0, i-5), i):  # Limit context to last 5 messages for speed
            ctx_msg = chain[j]
            ctx_content = preprocess_text_optimized(
                ctx_msg[2], participants, mention_pattern, role_pattern, channel_pattern, url_pattern
            )
            if ctx_content:
                context_messages.append(f"<@{ctx_msg[3]}>: {ctx_content}")

        context_str = "\n".join(context_messages) if context_messages else "[Aucun contexte]"
        current_formatted = f"<@{author_id}>: {processed_content}"
        next_formatted = f"<@{next_author_id}>: {processed_next_content}"

        user_content = format_user_content(context_str, current_formatted, user_template)
        model_content = format_model_content(next_formatted, model_template)

        total_new_chars = len(user_content) + len(model_content)
        if MAX_INPUT_CHARS != float('inf') and total_chars + total_new_chars > MAX_INPUT_CHARS:
            break

        conversation_parts.append({
            "role": "user",
            "parts": [{"text": user_content}]
        })
        conversation_parts.append({
            "role": "model",
            "parts": [{"text": model_content}]
        })

        total_chars += total_new_chars

    if len(conversation_parts) < 2:
        return None, 0

    return {"contents": conversation_parts}, total_chars


def preprocess_text_optimized(text, allowed_users, mention_pattern, role_pattern, channel_pattern, url_pattern):
    """
    Optimized text preprocessing with pre-compiled patterns.
    """
    if not isinstance(text, str) or not text.strip():
        return ""

    # Quick URL check
    if url_pattern.search(text):
        return ""

    # Quick length check
    if len(text) > 2000:  # Skip very long messages
        return ""

    # Apply replacements with pre-compiled patterns
    if allowed_users:
        text = mention_pattern.sub(
            lambda m: m.group(0) if m.group(1) in allowed_users else "@user", text
        )
    else:
        text = mention_pattern.sub("@user", text)

    text = role_pattern.sub("@role", text)
    text = channel_pattern.sub("#channel", text)

    # Quick emoji removal (simplified)
    text = re.sub(r":[a-zA-Z0-9_]{2,20}:", "", text)

    # Simplified formatting removal
    text = re.sub(r"(\*\*|__|\*|_|~~)(.*?)\1", lambda m: m.group(2), text)
    text = re.sub(r"\s+", " ", text).strip()

    return text if len(text.split()) >= 2 else ""


def write_chain_records_to_jsonl_optimized(
    chains: list,
    output_filepath: str,
    description: str,
    target_bot_id: str | None = None,
    user_template: str | None = None,
    model_template: str | None = None,
    max_workers: int = 16,
    batch_size: int = 2000,
):
    """
    Ultra-optimized version using aggressive parallel processing and memory management.
    """
    print(f"[*] Processing {len(chains)} chains to {output_filepath} using {max_workers} workers...")

    # Pre-filter chains for basic validity to reduce processing load
    print("[*] Pre-filtering chains...")
    valid_chains = []
    for chain in chains:
        if len(chain) >= 2:
            # Quick content check
            has_valid_content = True
            for msg in chain:
                if not msg[2] or len(msg[2].strip()) < 3:  # content at index 2
                    has_valid_content = False
                    break
            if has_valid_content:
                valid_chains.append(chain)

    print(f"[+] {len(valid_chains)} chains passed pre-filtering (removed {len(chains) - len(valid_chains)})")

    # Split chains into larger batches for better throughput
    chain_batches = []
    for i in range(0, len(valid_chains), batch_size):
        batch = valid_chains[i:i + batch_size]
        chain_batches.append(batch)

    valid_records_count = 0

    # Use a lock for thread-safe file writing
    write_lock = threading.Lock()

    with open(output_filepath, "w", encoding="utf-8") as f:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all batches for processing
            future_to_batch = {
                executor.submit(process_chain_batch_optimized, batch, target_bot_id, user_template, model_template): i
                for i, batch in enumerate(chain_batches)
            }

            # Process completed batches with progress bar
            with tqdm(total=len(chain_batches), desc=description, unit="batches") as pbar:
                for future in as_completed(future_to_batch):
                    try:
                        batch_records = future.result()

                        # Thread-safe file writing
                        with write_lock:
                            for record in batch_records:
                                f.write(json.dumps(record, ensure_ascii=False, separators=(',', ':')) + "\n")
                                valid_records_count += 1
                            f.flush()  # Ensure data is written

                    except Exception as e:
                        print(f"[WARNING] Batch processing failed: {e}")

                    pbar.update(1)

                    # Memory cleanup every 10 batches
                    if future_to_batch[future] % 10 == 0:
                        gc.collect()

    print(f"[+] {valid_records_count} valid records written to {output_filepath}.")
    return valid_records_count


def generate_datasets(
    db_dsn: str,
    train_path: str,
    valid_path: str,
    split_ratio: float,
    target_bot_id: str | None = None,
    user_template: str | None = None,
    model_template: str | None = None,
    max_workers: int = 8,
    batch_size: int = 1000,
):
    # Get message chains using optimized bulk loading
    all_chains = get_message_chains_from_db(db_dsn)

    if len(all_chains) < MIN_PAIRS_FOR_SPLIT:
        print(f"\n[WARNING] Less than {MIN_PAIRS_FOR_SPLIT} chains found.")
        print("[WARNING] All data will be written to the training file only.")
        write_chain_records_to_jsonl_optimized(
            all_chains, train_path, "Training", target_bot_id, user_template, model_template,
            max_workers, batch_size
        )
        open(valid_path, "w").close()
        return

    random.shuffle(all_chains)

    num_validation = int(len(all_chains) * split_ratio)
    num_validation = max(
        MIN_VALIDATION_EXAMPLES, min(num_validation, MAX_VALIDATION_EXAMPLES)
    )

    if num_validation >= len(all_chains):
        num_validation = len(all_chains) - MIN_VALIDATION_EXAMPLES

    validation_chains = all_chains[:num_validation]
    training_chains = all_chains[num_validation:]

    print(
        f"\n[INFO] {len(training_chains)} chains for training, {len(validation_chains)} for validation"
    )

    # Use optimized parallel processing for both datasets
    print("---")
    write_chain_records_to_jsonl_optimized(
        training_chains, train_path, "Training set", target_bot_id, user_template, model_template,
        max_workers, batch_size
    )
    print("---")
    write_chain_records_to_jsonl_optimized(
        validation_chains, valid_path, "Validation set", target_bot_id, user_template, model_template,
        max_workers, batch_size
    )
    print("\n[SUCCESS] Operation completed with optimized reply chains processing.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Optimized Discord reply chain processor for fine-tuning Gemini 2.0 Flash.\nPerformance optimized for 96GB RAM systems.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "db_dsn",
        help="Connection string (DSN) PostgreSQL.\nFormat: postgresql://user:pass@host:port/db",
    )
    parser.add_argument("training_output_file", help="Path to JSONL file for training.")
    parser.add_argument(
        "validation_output_file", help="Path to JSONL file for validation."
    )
    parser.add_argument(
        "--split-ratio", type=float, default=0.1, help="Validation ratio (default: 0.1)"
    )
    parser.add_argument(
        "--target-bot-id", type=str, help="Target Discord bot ID (optional)."
    )
    parser.add_argument(
        "--user-template",
        type=str,
        help="Custom template for user parts. Use {context_messages} and {current_message} placeholders."
    )
    parser.add_argument(
        "--model-template",
        type=str,
        help="Custom template for model parts. Use {response_message} placeholder."
    )
    parser.add_argument(
        "--max-workers", type=int, default=8,
        help="Number of parallel workers for processing (default: 8, optimized for 96GB RAM)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000,
        help="Batch size for parallel processing (default: 1000)"
    )

    args = parser.parse_args()

    if not 0 < args.split_ratio < 1:
        print("[ERROR] The split-ratio must be between 0 and 1.", file=sys.stderr)
        sys.exit(1)

    # Performance settings with turbo mode
    max_workers = args.max_workers
    batch_size = args.batch_size

    if args.turbo_mode:
        print("[🚀] TURBO MODE ENABLED - Maximum performance settings")
        max_workers = min(32, max_workers * 2)  # Double workers in turbo mode
        batch_size = min(5000, batch_size * 2)  # Larger batches
        print(f"[🚀] Turbo settings: {max_workers} workers, {batch_size} batch size")

    # Récupérer les templates personnalisés
    user_template = args.user_template
    model_template = args.model_template

    if user_template:
        print(f"[INFO] Using custom user template: {user_template[:50]}...")

    if model_template:
        print(f"[INFO] Using custom model template: {model_template[:50]}...")

    print(f"[INFO] Performance settings: {max_workers} workers, {batch_size} batch size")

    generate_datasets(
        args.db_dsn,
        args.training_output_file,
        args.validation_output_file,
        args.split_ratio,
        args.target_bot_id,
        user_template,
        model_template,
        max_workers,
        batch_size,
    )
