require 'gpgme'
require 'tempfile'

module GPG

    def gpg_decrypt(**options)
        local_filename = options[:local_filename]
        buffer = options[:buffer]
        password = options[:password]
        recipients = options[:recipients]

        self.gpg_load_keys

        crypto = GPGME::Crypto.new :always_trust => true
        encrypted_data = local_filename ? File.open(local_filename) : buffer

        return crypto.decrypt(encrypted_data, :password => password).to_s
    end

    def gpg_encrypt(**options)
        local_filename = options[:local_filename]
        buffer = options[:buffer]
        password = options[:password]
        recipients = options[:recipients]

        self.gpg_load_keys

        crypto = GPGME::Crypto.new :always_trust => true
        unencrypted_data = local_filename ? File.open(local_filename) : buffer

        return crypto.encrypt(unencrypted_data, :recipients => recipients).to_s
    end

    def gpg_load_keys(gpg_key_path: @gpg_key_path, **options)
        gpg_key = options[:gpg_key]
        puts "gpg_key_path #{gpg_key_path}"

        gpg_keys = File.join(gpg_key_path, "*.asc")
        Dir.glob(gpg_keys).map(&File.method(:realpath)).each do |item|
            next if item == '.' or item == '..'
            puts "import: #{item}"

            GPGME::Key.import(File.open(item))
        end
    end
end
