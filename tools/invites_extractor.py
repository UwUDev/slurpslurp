import sys
import re
import psycopg2
from urllib.parse import urlparse

def extract_invite_codes_from_db(db_url):
    result = urlparse(db_url)
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    port = result.port

    conn = psycopg2.connect(
        dbname=database,
        user=username,
        password=password,
        host=hostname,
        port=port
    )
    cur = conn.cursor()

    query = """
    SELECT messages.content FROM messages WHERE content ~ '(https?:\/\/)?(www\.)?((discordapp\.com\/invite)|(discord\.gg))\/(\w+)'
    """
    cur.execute(query)
    rows = cur.fetchall()

    invite_code_pattern = re.compile(r'(?:https?://)?(?:www\.)?(?:discordapp\.com/invite|discord\.gg)/(\w+)')

    invite_codes = []
    for row in rows:
        content = row[0]
        matches = invite_code_pattern.findall(content)
        invite_codes.extend(matches)

    invite_codes = list(set(invite_codes))

    with open('invites.txt', 'w') as f:
        for code in invite_codes:
            f.write(code + '\n')

    cur.close()
    conn.close()

    return invite_codes

if __name__ == "__main__":
    db_url = sys.argv[1] if len(sys.argv) > 1 else None
    if not db_url:
        print("Usage: python invites_extractor.py <database_url>")
        sys.exit(1)
    codes = extract_invite_codes_from_db(db_url)
    print(f"Found {len(codes)} unique invite codes:")
    for code in codes:
        print(f"  - {code}")
