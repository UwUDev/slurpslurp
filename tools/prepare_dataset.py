import psycopg2
import json
import re
import argparse
import sys
import random
from tqdm import tqdm
from datetime import datetime, timedelta

MAX_INPUT_CHARS = 35000
MAX_OUTPUT_CHARS = 5000
MIN_VALIDATION_EXAMPLES = 10
MAX_VALIDATION_EXAMPLES = 5000
MIN_PAIRS_FOR_SPLIT = 20
MAX_CONTEXT_MESSAGES = 5
CONTEXT_TIME_LIMIT_MINUTES = 30


def preprocess_text(text: str, allowed_users: set = None) -> str:
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

    # Unknown users and roles
    text = re.sub(r"<@&\d+>", "@role", text)
    text = re.sub(r"<#\d+>", "#channel", text)

    # Disallow URLs
    if re.search(r"https?://\S+", text):
        return ""

    # Disallow code blocks (to avoid people asking "CaN yOu Do My HoMeWoRk?")
    if re.search(r"``````", text, flags=re.DOTALL):
        return ""

    # Remove custom Discord emojis
    text = re.sub(r":[a-zA-Z0-9-_]{3,32}:", "", text)

    # Remove Markdown formatting etc..
    text = re.sub(r"(\*\*|__|\*|_|~~)(.*?)\1", r"\2", text)
    text = re.sub(r"<a?:(\w+):\d+>", r":\1:", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def is_meaningful_exchange(input_text: str, output_text: str) -> bool:
    if len(output_text.split()) < 3:
        return False

    # Messages with only special characters or numbers are useless
    reaction_patterns = [r'^[!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]+$', r"^[0-9]+$"]

    if any(
        re.match(pattern, output_text.lower().strip()) for pattern in reaction_patterns
    ):
        return False

    if input_text.lower().strip() == output_text.lower().strip():
        return False

    return True


def get_conversation_context(
    cursor,
    message_id: int,
    channel_id: int,
    message_timestamp,
    max_context: int = MAX_CONTEXT_MESSAGES,
) -> list:
    time_limit = message_timestamp - timedelta(minutes=CONTEXT_TIME_LIMIT_MINUTES)

    query = """
    SELECT content, author_id, created_at
    FROM messages
    WHERE channel_id = %s
    AND id < %s
    AND content IS NOT NULL
    AND content != ''
    AND created_at >= %s
    ORDER BY id DESC
    LIMIT %s
    """
    cursor.execute(query, (channel_id, message_id, time_limit, max_context))
    return list(reversed(cursor.fetchall()))


def get_conversation_participants(
    context_messages: list, input_author_id: str, output_author_id: str
) -> set:
    participants = {str(input_author_id), str(output_author_id)}

    for _, author_id, _ in context_messages:
        participants.add(str(author_id))

    return participants


def get_extended_context(
    cursor,
    message_id: int,
    channel_id: int,
    message_timestamp,
    invalid_messages: set,
    max_context: int = MAX_CONTEXT_MESSAGES,
) -> list:

    time_limit = message_timestamp - timedelta(minutes=CONTEXT_TIME_LIMIT_MINUTES)

    extended_limit = max_context * 2

    query = """
    SELECT content, author_id, created_at, id
    FROM messages
    WHERE channel_id = %s
    AND id < %s
    AND content IS NOT NULL
    AND content != ''
    AND created_at >= %s
    ORDER BY id DESC
    LIMIT %s
    """

    cursor.execute(query, (channel_id, message_id, time_limit, extended_limit))
    all_potential_messages = list(reversed(cursor.fetchall()))

    valid_context = []
    for msg_content, author_id, timestamp, msg_id in all_potential_messages:
        if msg_id in invalid_messages:
            continue

        processed_msg = preprocess_text(msg_content)
        if processed_msg and len(processed_msg.split()) >= 2:
            valid_context.append((msg_content, author_id, timestamp))

        if len(valid_context) >= max_context:
            break

    return valid_context


def get_message_pairs_from_db(db_dsn: str) -> list:
    print(f"[*] Connecting to PostgreSQL database...")
    try:
        with psycopg2.connect(db_dsn) as conn:
            with conn.cursor() as cursor:
                query = """
                SELECT
                    original_msg.id,
                    original_msg.channel_id,
                    original_msg.content AS input_text,
                    original_msg.author_id AS input_author_id,
                    original_msg.created_at AS input_timestamp,
                    reply_msg.content AS output_text,
                    reply_msg.author_id AS output_author_id
                FROM
                    messages AS reply_msg
                JOIN
                    messages AS original_msg
                ON
                    reply_msg.referenced_message_id = original_msg.id
                WHERE
                    reply_msg.content IS NOT NULL AND reply_msg.content != '' AND
                    original_msg.content IS NOT NULL AND original_msg.content != ''
                ORDER BY original_msg.created_at DESC;
                """
                cursor.execute(query)
                pairs = cursor.fetchall()
                print(f"[+] {len(pairs)} pairs of messages found in the database.")
                return pairs
    except psycopg2.Error as e:
        print(f"[ERROR] PostgreSQL database error : {e}", file=sys.stderr)
        sys.exit(1)


def determine_message_role(author_id: str, target_bot_id: str = None) -> str:
    if target_bot_id and str(author_id) == str(target_bot_id):
        return "model"
    return "user"


# Basically learn how to ping users based on the context
def create_contextual_record(
    message_data: tuple, context_messages: list, target_bot_id: str = None
) -> tuple:
    (
        msg_id,
        channel_id,
        input_content,
        input_author_id,
        input_timestamp,
        output_content,
        output_author_id,
    ) = message_data

    participants = get_conversation_participants(
        context_messages, input_author_id, output_author_id
    )

    conversation_text_parts = []
    total_chars = 0

    for msg_content, author_id, timestamp in context_messages:
        processed_msg = preprocess_text(msg_content, participants)
        if processed_msg:
            formatted_msg = f"<@{author_id}>: {processed_msg}"
            if total_chars + len(formatted_msg) < MAX_INPUT_CHARS:
                conversation_text_parts.append(formatted_msg)
                total_chars += len(formatted_msg)

    processed_input = preprocess_text(input_content, participants)
    processed_output = preprocess_text(output_content, participants)

    if processed_input:
        formatted_input = f"<@{input_author_id}>: {processed_input}"
        if total_chars + len(formatted_input) < MAX_INPUT_CHARS:
            conversation_text_parts.append(formatted_input)
            total_chars += len(formatted_input)

    full_conversation = "\n".join(conversation_text_parts)

    conversation_parts = []
    if full_conversation:
        conversation_parts.append(
            {"role": "user", "parts": [{"text": full_conversation}]}
        )

    if processed_output:
        conversation_parts.append(
            {"role": "model", "parts": [{"text": processed_output}]}
        )

    return {"contents": conversation_parts}, total_chars + len(processed_output)


def write_contextual_pairs_to_jsonl(
    pairs: list,
    output_filepath: str,
    description: str,
    db_dsn: str,
    target_bot_id: str = None,
):
    print(f"[*] Write {len(pairs)} pairs with Discord mentions in {output_filepath}...")
    valid_records_count = 0

    try:
        with psycopg2.connect(db_dsn) as conn:
            with conn.cursor() as cursor:
                with open(output_filepath, "w", encoding="utf-8") as f:
                    for pair_data in tqdm(pairs, desc=description):
                        if len(pair_data) < 7:
                            continue

                        (
                            msg_id,
                            channel_id,
                            input_content,
                            input_author_id,
                            input_timestamp,
                            output_content,
                            output_author_id,
                        ) = pair_data

                        processed_input = preprocess_text(input_content)
                        processed_output = preprocess_text(output_content)

                        if not processed_input or not processed_output:
                            continue

                        if not is_meaningful_exchange(
                            processed_input, processed_output
                        ):
                            continue

                        if len(processed_output) > MAX_OUTPUT_CHARS:
                            continue

                        context_messages = get_conversation_context(
                            cursor, msg_id, channel_id, input_timestamp
                        )

                        try:
                            json_record, total_length = create_contextual_record(
                                pair_data, context_messages, target_bot_id
                            )

                            if (
                                total_length <= MAX_INPUT_CHARS
                                and json_record["contents"]
                            ):
                                f.write(
                                    json.dumps(json_record, ensure_ascii=False) + "\n"
                                )
                                valid_records_count += 1

                        except Exception as e:
                            continue

    except psycopg2.Error as e:
        print(f"[ERROR] Database error : {e}", file=sys.stderr)
        return

    print(f"[+] {valid_records_count} valid records written to {output_filepath}.")


def generate_datasets(
    db_dsn: str,
    train_path: str,
    valid_path: str,
    split_ratio: float,
    target_bot_id: str = None,
):
    all_pairs = get_message_pairs_from_db(db_dsn)

    if len(all_pairs) < MIN_PAIRS_FOR_SPLIT:
        print(f"\n[WARNING] Less than {MIN_PAIRS_FOR_SPLIT} pairs found.")
        print("[WARNING] All data will be written to the training file only.")
        write_contextual_pairs_to_jsonl(
            all_pairs, train_path, "Training", db_dsn, target_bot_id
        )
        open(valid_path, "w").close()
        return

    random.shuffle(all_pairs)

    num_validation = int(len(all_pairs) * split_ratio)
    num_validation = max(
        MIN_VALIDATION_EXAMPLES, min(num_validation, MAX_VALIDATION_EXAMPLES)
    )

    if num_validation >= len(all_pairs):
        num_validation = len(all_pairs) - MIN_VALIDATION_EXAMPLES

    validation_pairs = all_pairs[:num_validation]
    training_pairs = all_pairs[num_validation:]

    print(
        f"\n[INFO] {len(training_pairs)} pairs for training, {len(validation_pairs)} for validation"
    )
    print("---")
    write_contextual_pairs_to_jsonl(
        training_pairs, train_path, "Training set", db_dsn, target_bot_id
    )
    print("---")
    write_contextual_pairs_to_jsonl(
        validation_pairs, valid_path, "Training set", db_dsn, target_bot_id
    )
    print("\n[SUCCESS] Operation completed with dynamic extended context.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepares Discord data with dynamic extended context for fine-tuning Gemini or other trainable models with jsonb.",
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
        "--max-context",
        type=int,
        default=MAX_CONTEXT_MESSAGES,
        help=f"Max number of context messages (default: {MAX_CONTEXT_MESSAGES}).",
    )

    args = parser.parse_args()

    if not 0 < args.split_ratio < 1:
        print("[ERROR] The split-ratio must be between 0 and 1.", file=sys.stderr)
        sys.exit(1)

    if args.max_context:
        MAX_CONTEXT_MESSAGES = args.max_context

    generate_datasets(
        args.db_dsn,
        args.training_output_file,
        args.validation_output_file,
        args.split_ratio,
        args.target_bot_id,
    )
