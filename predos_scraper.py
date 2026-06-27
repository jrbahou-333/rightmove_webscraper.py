import re
import csv
import sys

TIMESTAMP_RE = re.compile(
    r"^(\d{2}/\d{2}/\d{4}, \d{2}:\d{2}) - (.+?): (.+)$"
)
DAY_RE = re.compile(r"Day\s+(\d+)", re.IGNORECASE)
SCORE_RE = re.compile(r"^(.+?)\s+(\d+)\s*-\s*(\d+)\s+(.+)$")

EMOJI_RE = re.compile(
    "["
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U0001F300-\U0001F9FF"  # misc symbols/emojis
    "\U00002702-\U000027B0"
    "\U0000FE00-\U0000FE0F"
    "\U0000200D"
    "]+",
    flags=re.UNICODE,
)


def clean_team(name):
    name = EMOJI_RE.sub("", name)
    name = name.replace("<This message was edited>", "")
    return name.strip()


def parse_chat(filepath):
    messages = []
    current_sender = None
    current_lines = []

    with open(filepath, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            m = TIMESTAMP_RE.match(line)
            if m:
                if current_sender and current_lines:
                    messages.append((current_sender, current_lines))
                current_sender = m.group(2)
                current_lines = [m.group(3)]
            else:
                if current_sender:
                    current_lines.append(line)

    if current_sender and current_lines:
        messages.append((current_sender, current_lines))

    return messages


def extract_predictions(messages):
    rows = []
    for sender, lines in messages:
        full_text = "\n".join(lines)
        day_match = DAY_RE.search(full_text)
        if not day_match:
            continue

        day = int(day_match.group(1))

        for line in lines:
            score_match = SCORE_RE.match(line.strip())
            if score_match:
                home = clean_team(score_match.group(1))
                home_score = int(score_match.group(2))
                away_score = int(score_match.group(3))
                away = clean_team(score_match.group(4))
                if home and away:
                    rows.append({
                        "Name": sender,
                        "Day": day,
                        "Home Team": home,
                        "Home Score": home_score,
                        "Away Team": away,
                        "Away Score": away_score,
                    })

    return rows


def main():
    filepath = sys.argv[1] if len(sys.argv) > 1 else "chat.txt"
    messages = parse_chat(filepath)
    predictions = extract_predictions(messages)

    output = sys.argv[2] if len(sys.argv) > 2 else "predictions.csv"
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["Name", "Day", "Home Team", "Home Score", "Away Team", "Away Score"]
        )
        writer.writeheader()
        writer.writerows(predictions)

    print(f"Extracted {len(predictions)} predictions to {output}")


if __name__ == "__main__":
    main()
