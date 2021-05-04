# import rsa
#
# # 生成密钥
# pubkey, privkey = rsa.newkeys(1024)
#
# # 保存密钥
# print("==============保存密钥===============")
# pub_str = pubkey.save_pkcs1().decode()
# pri_str = privkey.save_pkcs1().decode()
# print(pub_str)
# print(pri_str)
#
# # 导入密钥
# print("==============导入密钥===============")
# pub_k = rsa.PublicKey.load_pkcs1(pub_str.encode())
# pri_k = rsa.PrivateKey.load_pkcs1(pri_str.encode())
#
# print(pub_k,pri_k)
# """
# 加密 RSA
# """
#
#
# def rsa_encrypt(message):
#     crypto_email_text = rsa.encrypt(message.encode(), pubkey)
#     return crypto_email_text
#
#
# text = rsa_encrypt('{"type": "request_vote_response", "src_id": "node_3", "dst_id": "node_2", "term": 43, "vote_granted": true}')
# print(text)
#
# """
# 解密
# """
#
#
# def rsa_decrypt(message):
#     message_str = rsa.decrypt(message, privkey).decode()
#     return message_str
#
#
# message = rsa_decrypt(text)
# print("\n", message)
ss = b'votes_response [B]\xcfvzh2y?\xf7K\x1c\xea\xe6z\xb9\x90\x03\xe9\x1b\n\x05\xe4s\xa7\xe6\xb5a\x14@\x0b\x02\x97\xa4\xf9"\xe8W\xa3:qNj\xd7\t1C\x96\xff\xc9o\xc9Qkg-\x02\xdb\x1bSR\xfd\xb2\xef f\x9e\x04q\xa7\xf8\xf9\x83X\xecH\xbel\xff\x1aU\x8b\x9f\x16d\x00\xca\xe8\x83\xe1\xc8\xd4\x1e`\n\xa1t\xb9\x93\xde.X\xb87{\x94\x19a\xe9\x12cmLH\xb4\x17v\x81\x17,\xd5\xae\xcb\x83\x1e\xb5y'

