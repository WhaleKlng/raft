import rsa


def rsa_encrypt(message, public_key):
    crypto_email_text = rsa.encrypt(message.encode(), public_key)
    return crypto_email_text


def rsa_decrypt(message, private_key):
    message_str = rsa.decrypt(message, private_key).decode()
    return message_str


if __name__ == '__main__':
    rsa_encrypt("我赞成，我是B", "Ad公钥")
