from consumer.errors import InvalidHeader


def header_value(headers, name, default=None):
    matches = [value for key, value in (headers or []) if key == name]
    return matches[-1] if matches else default


def integer_header(headers, name, default=0):
    value = header_value(headers, name, str(default).encode())
    try:
        text = value.decode("ascii") if isinstance(value, bytes) else value
        if not isinstance(text, str) or not text or not text.isascii() or not text.isdecimal():
            raise ValueError("not an unsigned decimal integer")
        result = int(text)
        if result > 1_000_000:
            raise ValueError("header exceeds supported limit")
        return result
    except (ValueError, UnicodeError) as error:
        raise InvalidHeader(f"Invalid {name} header: {value!r}") from error


def retry_count(headers):
    return integer_header(headers, "retry-count")


def replace_headers(headers, **updates):
    return [(key, value) for key, value in (headers or []) if key not in updates] + [
        (key, str(value).encode("utf-8")) for key, value in updates.items()
    ]