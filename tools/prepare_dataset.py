import psycopg2
import json
import re
import argparse
import sys
import random
from tqdm import tqdm
from datetime import datetime, timedelta

MAX_INPUT_CHARS = float('inf')  # Aucune limite
MAX_OUTPUT_CHARS = float('inf')  # Aucune limite
MIN_VALIDATION_EXAMPLES = 10
MAX_VALIDATION_EXAMPLES = 5000
MIN_PAIRS_FOR_SPLIT = 20
MAX_CONTEXT_MESSAGES = float('inf')  # Aucune limite
CONTEXT_TIME_LIMIT_MINUTES = float('inf')  # Aucune limite

# Templates customisables pour le contenu des parts
USER_PART_TEMPLATE = """CONTEXT:
{context_messages}
---
CURRENT: {current_message}"""

MODEL_PART_TEMPLATE = """{response_message}"""

# Fonctions pour utiliser les templates
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
    max_context: int | float = MAX_CONTEXT_MESSAGES,
) -> list:
    # Pas de limite de temps si CONTEXT_TIME_LIMIT_MINUTES est infini
    if CONTEXT_TIME_LIMIT_MINUTES == float('inf'):
        time_limit = datetime.min  # Date très ancienne pour récupérer tout
    else:
        time_limit = message_timestamp - timedelta(minutes=CONTEXT_TIME_LIMIT_MINUTES)

    # Construire la requête avec ou sans LIMIT
    if max_context == float('inf'):
        query = """
        SELECT content, author_id, created_at
        FROM messages
        WHERE channel_id = %s
        AND id < %s
        AND content IS NOT NULL
        AND content != ''
        AND created_at >= %s
        ORDER BY id DESC
        """
        cursor.execute(query, (channel_id, message_id, time_limit))
    else:
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
    max_context: int | float = MAX_CONTEXT_MESSAGES,
) -> list:
    # Pas de limite de temps si CONTEXT_TIME_LIMIT_MINUTES est infini
    if CONTEXT_TIME_LIMIT_MINUTES == float('inf'):
        time_limit = datetime.min  # Date très ancienne pour récupérer tout
    else:
        time_limit = message_timestamp - timedelta(minutes=CONTEXT_TIME_LIMIT_MINUTES)

    # Pas de limite si max_context est infini
    if max_context == float('inf'):
        extended_limit = None  # Pas de limite
    else:
        extended_limit = max_context * 2

    # Construire la requête avec ou sans LIMIT
    if extended_limit is None:
        query = """
        SELECT content, author_id, created_at, id
        FROM messages
        WHERE channel_id = %s
        AND id < %s
        AND content IS NOT NULL
        AND content != ''
        AND created_at >= %s
        ORDER BY id DESC
        """
        cursor.execute(query, (channel_id, message_id, time_limit))
    else:
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

        # Ne pas limiter si max_context est infini
        if max_context != float('inf') and len(valid_context) >= max_context:
            break

    return valid_context


def get_message_chains_from_db(db_dsn: str) -> list:
    """
    Récupère tous les messages qui ont au moins une réponse,
    puis construit les chaînes de réponses complètes.
    """
    print(f"[*] Connecting to PostgreSQL database...")
    try:
        with psycopg2.connect(db_dsn) as conn:
            with conn.cursor() as cursor:
                # D'abord, récupérer tous les messages qui ont au moins une réponse
                query_root_messages = """
                SELECT DISTINCT m.id, m.channel_id, m.content, m.author_id, m.created_at
                FROM messages m
                WHERE EXISTS (
                    SELECT 1 FROM messages r
                    WHERE r.referenced_message_id = m.id
                    AND r.content IS NOT NULL
                    AND r.content != ''
                )
                AND m.content IS NOT NULL
                AND m.content != ''
                ORDER BY m.created_at DESC;
                """
                cursor.execute(query_root_messages)
                root_messages = cursor.fetchall()
                print(f"[+] {len(root_messages)} messages with replies found.")

                # Pour chaque message racine, construire la chaîne complète de réponses
                all_chains = []
                for root_msg in tqdm(root_messages, desc="Building reply chains"):
                    chain = build_reply_chain(cursor, root_msg)
                    if len(chain) >= 2:  # Au moins un message + une réponse
                        all_chains.append(chain)

                print(f"[+] {len(all_chains)} complete reply chains built.")
                return all_chains

    except psycopg2.Error as e:
        print(f"[ERROR] PostgreSQL database error : {e}", file=sys.stderr)
        sys.exit(1)


def build_reply_chain(cursor, root_message: tuple) -> list:
    """
    Construit une chaîne complète de réponses à partir d'un message racine.
    Retourne une liste de messages dans l'ordre chronologique.
    """
    chain = [root_message]
    current_msg_id = root_message[0]

    while True:
        # Chercher toutes les réponses directes au message courant
        query_replies = """
        SELECT id, channel_id, content, author_id, created_at
        FROM messages
        WHERE referenced_message_id = %s
        AND content IS NOT NULL
        AND content != ''
        ORDER BY created_at ASC;
        """
        cursor.execute(query_replies, (current_msg_id,))
        replies = cursor.fetchall()

        if not replies:
            break

        # Ajouter toutes les réponses à la chaîne
        chain.extend(replies)

        # Pour continuer la chaîne, prendre le dernier message ajouté
        # (celui qui pourrait avoir des réponses à son tour)
        current_msg_id = replies[-1][0]

    return chain


def create_chain_record(chain: list, target_bot_id: str | None = None, user_template: str | None = None, model_template: str | None = None) -> tuple:
    """
    Crée un enregistrement d'entraînement à partir d'une chaîne de messages.
    La chaîne est formatée comme une conversation multi-tour utilisant les templates.
    """
    if len(chain) < 2:
        return None, 0

    # Obtenir tous les participants de la chaîne
    participants = set()
    for msg in chain:
        participants.add(str(msg[3]))  # author_id est à l'index 3

    conversation_parts = []
    total_chars = 0

    # Traiter chaque paire de messages (contexte + réponse)
    for i in range(len(chain) - 1):
        current_msg = chain[i]
        next_msg = chain[i + 1]

        msg_id, channel_id, content, author_id, timestamp = current_msg
        next_msg_id, _, next_content, next_author_id, _ = next_msg

        processed_content = preprocess_text(content, participants)
        processed_next_content = preprocess_text(next_content, participants)

        if not processed_content or not processed_next_content:
            continue

        # Construire le contexte (messages précédents) - TOUS les messages précédents
        context_messages = []
        for j in range(0, i):  # Prendre TOUS les messages précédents
            ctx_msg = chain[j]
            ctx_content = preprocess_text(ctx_msg[2], participants)
            if ctx_content:
                context_messages.append(f"<@{ctx_msg[3]}>: {ctx_content}")

        context_str = "\n".join(context_messages) if context_messages else "[Aucun contexte]"
        current_formatted = f"<@{author_id}>: {processed_content}"
        next_formatted = f"<@{next_author_id}>: {processed_next_content}"

        # Déterminer les rôles
        user_role = "user"
        model_role = "model"
        if target_bot_id:
            if str(next_author_id) == str(target_bot_id):
                model_role = "model"
            else:
                model_role = "user"
                user_role = "user"

        # Formater avec les templates
        user_content = format_user_content(context_str, current_formatted, user_template)
        model_content = format_model_content(next_formatted, model_template)

        # Vérifier la limite de caractères (ne pas limiter si MAX_INPUT_CHARS est infini)
        total_new_chars = len(user_content) + len(model_content)
        if MAX_INPUT_CHARS != float('inf') and total_chars + total_new_chars > MAX_INPUT_CHARS:
            break

        # Ajouter la paire user/model
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


def write_chain_records_to_jsonl(
    chains: list,
    output_filepath: str,
    description: str,
    target_bot_id: str | None = None,
    user_template: str | None = None,
    model_template: str | None = None,
):
    """
    Écrit les chaînes de messages au format JSONL pour l'entraînement.
    """
    print(f"[*] Write {len(chains)} chains to {output_filepath}...")
    valid_records_count = 0

    with open(output_filepath, "w", encoding="utf-8") as f:
        for chain in tqdm(chains, desc=description):
            if len(chain) < 2:
                continue

            try:
                json_record, total_length = create_chain_record(chain, target_bot_id, user_template, model_template)

                if json_record and json_record["contents"]:
                    # Vérifier que la chaîne contient au moins un échange significatif
                    if len(json_record["contents"]) >= 2:
                        # Vérifier que le dernier message est une réponse significative
                        last_content = json_record["contents"][-1]["parts"][0]["text"]
                        first_content = json_record["contents"][0]["parts"][0]["text"]

                        # Extraire le contenu sans les mentions pour la vérification
                        last_clean = last_content.split(": ", 1)[-1] if ": " in last_content else last_content
                        first_clean = first_content.split(": ", 1)[-1] if ": " in first_content else first_content

                        if is_meaningful_exchange(first_clean, last_clean):
                            f.write(json.dumps(json_record, ensure_ascii=False) + "\n")
                            valid_records_count += 1

            except Exception as e:
                continue

    print(f"[+] {valid_records_count} valid records written to {output_filepath}.")


def generate_datasets(
    db_dsn: str,
    train_path: str,
    valid_path: str,
    split_ratio: float,
    target_bot_id: str | None = None,
    user_template: str | None = None,
    model_template: str | None = None,
):
    all_chains = get_message_chains_from_db(db_dsn)

    if len(all_chains) < MIN_PAIRS_FOR_SPLIT:
        print(f"\n[WARNING] Less than {MIN_PAIRS_FOR_SPLIT} chains found.")
        print("[WARNING] All data will be written to the training file only.")
        write_chain_records_to_jsonl(
            all_chains, train_path, "Training", target_bot_id, user_template, model_template
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
    print("---")
    write_chain_records_to_jsonl(
        training_chains, train_path, "Training set", target_bot_id, user_template, model_template
    )
    print("---")
    write_chain_records_to_jsonl(
        validation_chains, valid_path, "Validation set", target_bot_id, user_template, model_template
    )
    print("\n[SUCCESS] Operation completed with reply chains.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepares Discord reply chains for fine-tuning Gemini 2.0 Flash.",
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

    args = parser.parse_args()

    if not 0 < args.split_ratio < 1:
        print("[ERROR] The split-ratio must be between 0 and 1.", file=sys.stderr)
        sys.exit(1)

    # Récupérer les templates personnalisés
    user_template = args.user_template
    model_template = args.model_template

    if user_template:
        print(f"[INFO] Using custom user template: {user_template[:50]}...")

    if model_template:
        print(f"[INFO] Using custom model template: {model_template[:50]}...")

    generate_datasets(
        args.db_dsn,
        args.training_output_file,
        args.validation_output_file,
        args.split_ratio,
        args.target_bot_id,
        user_template,
        model_template,
    )
