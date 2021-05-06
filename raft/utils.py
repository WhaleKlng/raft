import re


def check_is_phone(tel):
    return bool(re.match(r"^1[35678]\d{9}$", str(tel)))


