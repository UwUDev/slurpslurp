import psycopg2
import json
import argparse
import sys
import random
from tqdm import tqdm

MAX_INPUT_CHARS = 35000
MAX_OUTPUT_CHARS = 5000
MAX_CHAIN_LENGTH = 10
MAX_CHAINS = 100

def preprocess_text(text: str, author_id_to_role: dict = None) -> str:
    if not isinstance(text, str):
        return ""

    import re

    if author_id_to_role:
        def replace_mention(match):
            mentioned_id = match.group(1)
            if mentioned_id in author_id_to_role:
                return f"@{author_id_to_role[mentioned_id]}"
            else:
                return ""

        text = re.sub(r"<@!?(\d+)>", replace_mention, text)
    else:
        text = re.sub(r"<@!?\d+>", "@user", text)

    text = re.sub(r"<@&\d+>", "@role", text)
    text = re.sub(r"<#\d+>", "#channel", text)

    if re.search(r"https?://\S+", text):
        return ""

    if re.search(r"``````", text, flags=re.DOTALL):
        return ""

    text = re.sub(r"<:[a-zA-Z0-9-_]{2,32}:\d+>", "", text)

    text = re.sub(r"\s+", " ", text).strip()

    return text

def assign_last_speaker_as_assistant(messages):
    if not messages:
        return messages

    assistant_already_exists = any(msg["role"] == "assistant" for msg in messages)

    last_role = messages[-1]["role"]

    if assistant_already_exists:
        messages[-1]["role"] = "assistant"
    else:
        for msg in messages:
            if msg["role"] == last_role:
                msg["role"] = "assistant"

    return messages

def get_reply_chains(db_dsn: str, min_chain_length: int = 2) -> list:
    print(f"[*] Connecting to PostgreSQL database...")

    try:
        with psycopg2.connect(db_dsn) as conn:
            with conn.cursor() as cursor:
                query = """
                WITH RECURSIVE reply_chains AS (
                    SELECT
                        m.id,
                        m.channel_id,
                        m.author_id,
                        m.content,
                        u.username,
                        m.id as root_id,
                        1 as depth,
                        ARRAY[m.id] as chain_path,
                        ARRAY[m.id] as msg_ids,
                        ARRAY[m.author_id] as author_ids,
                        ARRAY[u.username] as usernames,
                        ARRAY[m.content] as contents
                    FROM messages m
                    JOIN users u ON m.author_id = u.id
                    WHERE m.referenced_message_id IS NULL
                      AND m.content IS NOT NULL
                      AND length(trim(m.content)) > 0
                      AND m.deleted_at IS NULL

                    UNION ALL

                    SELECT
                        reply.id,
                        reply.channel_id,
                        reply.author_id,
                        reply.content,
                        reply_user.username,
                        rc.root_id,
                        rc.depth + 1,
                        rc.chain_path || reply.id,
                        rc.msg_ids || reply.id,
                        rc.author_ids || reply.author_id,
                        rc.usernames || reply_user.username,
                        rc.contents || reply.content
                    FROM messages reply
                    JOIN users reply_user ON reply.author_id = reply_user.id
                    JOIN reply_chains rc ON reply.referenced_message_id = rc.id
                    WHERE reply.content IS NOT NULL
                      AND length(trim(reply.content)) > 0
                      AND reply.deleted_at IS NULL
                      AND rc.depth < %s
                      AND NOT (reply.id = ANY(rc.chain_path))
                )
                SELECT
                    root_id,
                    channel_id,
                    depth,
                    msg_ids,
                    author_ids,
                    usernames,
                    contents
                FROM reply_chains
                WHERE depth >= %s  -- Use the min_chain_length parameter
                ORDER BY root_id, depth DESC
                LIMIT %s;
                """

                cursor.execute(query, (MAX_CHAIN_LENGTH, min_chain_length, MAX_CHAINS * 2))
                chains = cursor.fetchall()

                print(f"[+] {len(chains)} chains of at least {min_chain_length} messages found.")
                return chains

    except psycopg2.Error as e:
        print(f"[ERROR] PostgreSQL error: {e}", file=sys.stderr)
        sys.exit(1)

def create_conversation_record(chain_data: tuple) -> dict:
    try:
        root_id, channel_id, depth, msg_ids, author_ids, usernames, contents = chain_data

        messages = []
        person_mapping = {}
        person_counter = 0

        if len(msg_ids) != len(author_ids) or len(author_ids) != len(usernames) or len(usernames) != len(contents):
            return None

        for i in range(len(msg_ids)):
            author_id = author_ids[i]

            if author_id not in person_mapping:
                person_mapping[author_id] = f"Person{chr(65 + person_counter)}"
                person_counter += 1

        author_id_to_role = {str(author_id): role for author_id, role in person_mapping.items()}

        for i in range(len(msg_ids)):
            author_id = author_ids[i]
            username = usernames[i]
            content = contents[i]

            processed_content = preprocess_text(content, author_id_to_role)
            if not processed_content or len(processed_content.strip()) < 2:
                continue

            role = person_mapping[author_id]

            messages.append({
                "role": role,
                "content": processed_content
            })

        if len(messages) < 2:
            return None

        messages = assign_last_speaker_as_assistant(messages)

        final_author_id_to_role = {}
        for i, msg in enumerate(messages):
            original_author_id = str(author_ids[i])
            final_author_id_to_role[original_author_id] = msg["role"]

        if final_author_id_to_role != author_id_to_role:
            for i, msg in enumerate(messages):
                original_content = contents[i]
                reprocessed_content = preprocess_text(original_content, final_author_id_to_role)
                if reprocessed_content and len(reprocessed_content.strip()) >= 2:
                    msg["content"] = reprocessed_content

        total_length = sum(len(msg["content"]) for msg in messages)
        if total_length > MAX_INPUT_CHARS:
            return None

        return {"messages": messages}

    except Exception as e:
        return None

def write_chains_to_jsonl(chains: list, output_filepath: str):
    """Writes conversation chains to JSONL format"""
    print(f"[*] Writing {len(chains)} chains to {output_filepath}...")

    valid_records_count = 0
    unique_chains = {}

    with open(output_filepath, "w", encoding="utf-8") as f:
        for chain_data in tqdm(chains, desc="Processing chains"):
            try:
                record = create_conversation_record(chain_data)
                if record and len(record["messages"]) >= 2:
                    root_id = chain_data[0]
                    if root_id not in unique_chains:
                        unique_chains[root_id] = record
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                        valid_records_count += 1

                        # Debug: display some examples
                        if valid_records_count <= 3:
                            print(f"[DEBUG] Example chain #{valid_records_count}:")
                            for msg in record['messages']:
                                print(f"  - {msg['role']}: {msg['content'][:50]}...")

                        if valid_records_count >= MAX_CHAINS:
                            break

            except Exception as e:
                continue

    print(f"[+] {valid_records_count} valid chains written to {output_filepath}.")

def generate_reply_chains_dataset(
    db_dsn: str,
    output_path: str,
    max_chains: int = MAX_CHAINS,
    min_chain_length: int = 2
):
    global MAX_CHAINS
    MAX_CHAINS = max_chains

    chains = get_reply_chains(db_dsn, min_chain_length)

    if not chains:
        print(f"[WARNING] No chains of at least {min_chain_length} messages found.")
        return

    write_chains_to_jsonl(chains, output_path)
    print(f"\n[SUCCESS] Dataset generated successfully: {output_path}")
    print(f"[INFO] Chains with at least {min_chain_length} messages")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generates a dataset of Discord reply chains in multi-character JSONL format with smart mentions.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "db_dsn",
        help="PostgreSQL connection string.\nFormat: postgresql://user:pass@host:port/db",
    )

    parser.add_argument(
        "output_file",
        help="Path to the output JSONL file."
    )

    parser.add_argument(
        "--max-chains",
        type=int,
        default=MAX_CHAINS,
        help=f"Maximum number of chains to generate (default: {MAX_CHAINS}).",
    )

    parser.add_argument(
        "--max-chain-length",
        type=int,
        default=MAX_CHAIN_LENGTH,
        help=f"Maximum length of a chain (default: {MAX_CHAIN_LENGTH}).",
    )

    parser.add_argument(
        "--min-chain-length",
        type=int,
        default=2,
        help="Minimum number of messages required in a chain (default: 2)."
    )

    args = parser.parse_args()

    if args.max_chain_length:
        MAX_CHAIN_LENGTH = args.max_chain_length

    if args.min_chain_length > args.max_chain_length:
        print(f"[ERROR] min-chain-length ({args.min_chain_length}) cannot be greater than max-chain-length ({args.max_chain_length})", file=sys.stderr)
        sys.exit(1)

    if args.min_chain_length < 2:
        print(f"[ERROR] min-chain-length must be at least 2", file=sys.stderr)
        sys.exit(1)

    generate_reply_chains_dataset(
        args.db_dsn,
        args.output_file,
        args.max_chains,
        args.min_chain_length
    )
