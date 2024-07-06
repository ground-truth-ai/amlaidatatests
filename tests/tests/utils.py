import random
import string


def temporary_table_name():
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(10))
