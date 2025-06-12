import re
import psycopg2
from urllib.parse import urlparse

def extract_invite_codes_from_db(db_url):
    # Parse the database URL
    result = urlparse(db_url)
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    port = result.port

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=database,
        user=username,
        password=password,
        host=hostname,
        port=port
    )
    cur = conn.cursor()

    # SQL query to get messages with Discord invite URLs
    query = """
    SELECT messages.content FROM messages WHERE content ~ '(https?:\/\/)?(www\.)?((discordapp\.com\/invite)|(discord\.gg))\/(\w+)'
    """
    cur.execute(query)
    rows = cur.fetchall()

    # Regex to extract invite codes from URLs
    invite_code_pattern = re.compile(r'(?:https?://)?(?:www\.)?(?:discordapp\.com/invite|discord\.gg)/(\w+)')

    invite_codes = []
    for row in rows:
        content = row[0]  # Extract content from tuple
        matches = invite_code_pattern.findall(content)
        invite_codes.extend(matches)

    # Remove duplicates
    invite_codes = list(set(invite_codes))

    # Save invite codes to invites.txt
    with open('invites.txt', 'w') as f:
        for code in invite_codes:
            f.write(code + '\n')

    cur.close()
    conn.close()

    return invite_codes

# Example usage
if __name__ == "__main__":
    db_url = 'postgresql://postgres:postgres@localhost:5432/slurpslurp'
    codes = extract_invite_codes_from_db(db_url)
    print(f"Found {len(codes)} unique invite codes:")
    for code in codes:
        print(f"  - {code}")
