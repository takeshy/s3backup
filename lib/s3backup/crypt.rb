require 'openssl'
module S3backup
  class Crypt
    CIPHER_ALGORITHM="aes-256-cbc"
    def initialize(password,salt)
      @password = password
      @salt = salt.scan(/../).map{|i|i.hex}.pack("c*")
    end
    def encrypt(data)
      enc = OpenSSL::Cipher::Cipher.new(CIPHER_ALGORITHM)
      enc.encrypt
      enc.pkcs5_keyivgen(@password,@salt)
      enc.update(data)+enc.final
    end
    def decrypt(data)
      enc = OpenSSL::Cipher::Cipher.new(CIPHER_ALGORITHM)
      enc.decrypt
      enc.pkcs5_keyivgen(@password,@salt)
      enc.update(data)+enc.final
    end
  end
end
