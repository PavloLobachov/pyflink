import uuid
import random

adjectives = [
    "quick", "bright", "calm", "eager", "fancy", "jolly",
    "kind", "lively", "nice", "proud", "silly", "witty",
    "zany", "bold", "charming", "daring", "happy"
]

nouns = [
    "lion", "tiger", "bear", "wolf", "fox", "eagle",
    "shark", "whale", "dolphin", "falcon", "panda",
    "hawk", "otter", "raccoon", "koala", "puma", "owl"
]


def generate_random_name():
    new_uuid = uuid.uuid4()
    uuid_str = str(new_uuid).replace('-', '')

    random.seed(uuid_str)
    adjective = random.choice(adjectives)
    noun = random.choice(nouns)

    random_name = f"{adjective}_{noun}"

    if len(random_name) > 14:
        random_name = random_name[:14]

    return random_name
